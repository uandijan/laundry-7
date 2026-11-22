const {onCall, HttpsError} = require('firebase-functions/v2/https');
const {onSchedule} = require('firebase-functions/v2/scheduler');
const {initializeApp} = require('firebase-admin/app');
const {getAuth} = require('firebase-admin/auth');
const {getFirestore, FieldValue} = require('firebase-admin/firestore');

initializeApp();
const db = getFirestore();
const auth = getAuth();

async function removeMatching(collection, field, uid) {
  while (true) {
    const snap = await db.collection(collection).where(field, '==', uid).limit(300).get();
    if (snap.empty) return;
    const batch = db.batch();
    for (const item of snap.docs) batch.delete(item.ref);
    await batch.commit();
  }
}

async function anonymizeMatching(collection, field, uid, patch) {
  while (true) {
    const snap = await db.collection(collection).where(field, '==', uid).limit(300).get();
    if (snap.empty) return;
    const batch = db.batch();
    for (const item of snap.docs) batch.update(item.ref, patch);
    await batch.commit();
  }
}

async function eraseStudent(uid) {
  const marker = db.doc(`deleted_users/${uid}`);
  await marker.set({status: 'deleting', updated_at: FieldValue.serverTimestamp()}, {merge: true});

  // Revoke access before deleting related documents. A repeated call is safe.
  try { await auth.deleteUser(uid); }
  catch (error) { if (error.code !== 'auth/user-not-found') throw error; }

  // Remove each reservation and its occupancy marker in the same commit.
  while (true) {
    const bookings = await db.collection('bookings').where('uid', '==', uid).limit(200).get();
    if (bookings.empty) break;
    const batch = db.batch();
    for (const booking of bookings.docs) {
      batch.delete(booking.ref);
      batch.delete(db.doc(`laundry_slots/${booking.id}`));
    }
    await batch.commit();
  }
  await removeMatching('booking_history', 'uid', uid);
  for (const collection of ['bookings', 'booking_history']) {
    await anonymizeMatching(collection, 'booked_by', uid, {
      booked_by: FieldValue.delete(), booked_by_role: FieldValue.delete()
    });
  }
  await anonymizeMatching('booking_history', 'cancelled_by', uid, {
    cancelled_by: FieldValue.delete()
  });
  for (const collection of ['brs_transactions', 'brs_audit', 'brs_view']) {
    await removeMatching(collection, 'student_uid', uid);
  }
  await anonymizeMatching('brs_audit', 'created_by', uid, {
    created_by: FieldValue.delete(), created_by_name: FieldValue.delete(),
    created_by_role: FieldValue.delete()
  });
  for (const collection of ['brs_audit', 'brs_transactions']) {
    await anonymizeMatching(collection, 'cancelled_by', uid, {
      cancelled_by: FieldValue.delete(), cancelled_by_name: FieldValue.delete()
    });
  }

  // Copies of a resident's operations may belong to multiple BRS managers.
  const managers = await db.collection('brs_manager_ops').listDocuments();
  for (const manager of managers) {
    while (true) {
      const snap = await manager.collection('items').where('cancelled_by', '==', uid).limit(300).get();
      if (snap.empty) break;
      const batch = db.batch();
      for (const item of snap.docs) batch.update(item.ref, {
        cancelled_by: FieldValue.delete(), cancelled_by_name: FieldValue.delete()
      });
      await batch.commit();
    }
    while (true) {
      const snap = await manager.collection('items').where('student_uid', '==', uid).limit(300).get();
      if (snap.empty) break;
      const batch = db.batch();
      for (const item of snap.docs) batch.delete(item.ref);
      await batch.commit();
    }
  }
  await db.recursiveDelete(db.doc(`brs_manager_ops/${uid}`));
  await db.doc(`brs_profiles/${uid}`).delete();
  await db.doc(`users/${uid}`).delete();

  // Verify the known collections before reporting success.
  for (const collection of ['bookings', 'booking_history']) {
    for (const field of ['uid', 'booked_by']) {
      if (!(await db.collection(collection).where(field, '==', uid).limit(1).get()).empty)
        throw new Error(`Remaining ${collection} record`);
    }
  }
  if (!(await db.collection('booking_history').where('cancelled_by', '==', uid).limit(1).get()).empty)
    throw new Error('Remaining booking cancellation author');
  for (const collection of ['brs_transactions', 'brs_audit', 'brs_view']) {
    if (!(await db.collection(collection).where('student_uid', '==', uid).limit(1).get()).empty)
      throw new Error(`Remaining ${collection} record`);
  }
  if (!(await db.collection('brs_audit').where('created_by', '==', uid).limit(1).get()).empty)
    throw new Error('Remaining BRS author');
  for (const collection of ['brs_audit', 'brs_transactions']) {
    if (!(await db.collection(collection).where('cancelled_by', '==', uid).limit(1).get()).empty)
      throw new Error(`Remaining ${collection} cancellation author`);
  }
  for (const manager of managers) {
    if (!(await manager.collection('items').where('student_uid', '==', uid).limit(1).get()).empty)
      throw new Error('Remaining manager record');
    if (!(await manager.collection('items').where('cancelled_by', '==', uid).limit(1).get()).empty)
      throw new Error('Remaining manager cancellation author');
  }
  await marker.set({status: 'complete', completed_at: FieldValue.serverTimestamp()}, {merge: true});
}

exports.deleteResident = onCall({timeoutSeconds: 540}, async (request) => {
  const actor = request.auth?.uid;
  const uid = request.data?.uid;
  if (!actor) throw new HttpsError('unauthenticated', 'Войдите в аккаунт.');
  if (typeof uid !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(uid) || uid === actor)
    throw new HttpsError('invalid-argument', 'Неверный аккаунт для удаления.');
  const actorDoc = await db.doc(`users/${actor}`).get();
  if (actorDoc.data()?.role !== 'admin')
    throw new HttpsError('permission-denied', 'Удалять жильцов может только администратор.');
  const target = await db.doc(`users/${uid}`).get();
  const marker = await db.doc(`deleted_users/${uid}`).get();
  if (!target.exists && !marker.exists)
    throw new HttpsError('not-found', 'Жилец не найден.');
  try {
    await eraseStudent(uid);
    return {complete: true};
  } catch (error) {
    console.error('Resident deletion failed', {uid, error});
    throw new HttpsError('internal', 'Удаление не завершено. Повторите попытку.');
  }
});

exports.retryResidentDeletions = onSchedule('every 24 hours', async () => {
  const pending = await db.collection('deleted_users').where('status', '==', 'deleting').get();
  for (const item of pending.docs) {
    try { await eraseStudent(item.id); }
    catch (error) { console.error('Scheduled resident deletion failed', {uid: item.id, error}); }
  }
});

const laundryClock = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Europe/Moscow', year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', hourCycle: 'h23'
});
const laundrySlotId = /^\d{4}-\d{2}-\d{2}_(?:0[6-9]|1[0-9]|2[0-2])00_m[1-5]$/;

function laundryNow() {
  const parts = Object.fromEntries(laundryClock.formatToParts(new Date()).map(p => [p.type, p.value]));
  return {date: `${parts.year}-${parts.month}-${parts.day}`, minutes: +parts.hour * 60 + +parts.minute};
}

function laundryActor(request) {
  if (!request.auth?.uid) throw new HttpsError('unauthenticated', 'Войдите в аккаунт.');
  return request.auth.uid;
}

exports.reserveLaundry = onCall(async request => {
  const actor = laundryActor(request);
  const {slots, targetUid = actor} = request.data || {};
  if (!Array.isArray(slots) || slots.length < 1 || slots.length > 20 ||
      typeof targetUid !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(targetUid)) {
    throw new HttpsError('invalid-argument', 'Проверьте выбранные записи.');
  }

  return db.runTransaction(async tx => {
    const actorRef = db.doc(`users/${actor}`);
    const actorDoc = await tx.get(actorRef);
    const actorData = actorDoc.data();
    const other = targetUid !== actor;
    const target = other ? (await tx.get(db.doc(`users/${targetUid}`))).data() : actorData;
    if (!actorData || (await tx.get(db.doc(`deleted_users/${actor}`))).exists ||
        (other && (await tx.get(db.doc(`deleted_users/${targetUid}`))).exists)) {
      throw new HttpsError('permission-denied', 'Аккаунт недоступен.');
    }
    const admin = actorData.role === 'admin';
    const council = actorData.role === 'student_council';
    if (other && !admin && !(council && actorData.view_students === true)) {
      throw new HttpsError('permission-denied', 'Нет права записывать этого жильца.');
    }
    if (!target?.name || !target.room || (other && target.role === 'admin')) {
      throw new HttpsError('failed-precondition', 'Профиль жильца недоступен или не заполнен.');
    }

    const now = laundryNow();
    const today = new Date(`${now.date}T00:00:00Z`);
    const lastDay = new Date(today);
    lastDay.setUTCDate(today.getUTCDate() + ({4: 3, 5: 2}[today.getUTCDay()] || 1));
    const records = slots.map(slot => {
      const {date, h, m} = slot || {};
      if (typeof date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(date) ||
          !Number.isInteger(h) || h < 6 || h > 22 || !Number.isInteger(m) || m < 1 || m > 5) {
        throw new HttpsError('invalid-argument', 'Неверное время или машина.');
      }
      const day = new Date(`${date}T00:00:00Z`);
      if (!Number.isFinite(+day) || day.toISOString().slice(0, 10) !== date ||
          day < today || day > lastDay || (date === now.date && now.minutes >= h * 60 + 30)) {
        throw new HttpsError('failed-precondition', 'Это время уже недоступно.');
      }
      const weekend = [0, 6].includes(day.getUTCDay());
      if ((!admin && !council && weekend) || (other && !admin && !weekend)) {
        throw new HttpsError('permission-denied', 'Не хватает прав для этого времени.');
      }
      const hour = String(h).padStart(2, '0');
      const id = `${date}_${hour}00_m${m}`;
      return {id, publicSlot: {date, start_time: `${hour}:00`, machine_number: m}};
    });
    if (new Set(records.map(record => record.id)).size !== records.length) {
      throw new HttpsError('invalid-argument', 'Выбраны повторяющиеся ячейки.');
    }

    if (!admin) {
      const bookings = db.collection('bookings');
      const created = await tx.get(bookings.where('booked_by', '==', actor));
      const owned = await tx.get(bookings.where('uid', '==', actor));
      const existing = [...created.docs, ...owned.docs.filter(doc => !doc.get('booked_by'))];
      const active = existing.filter(doc => {
        const {date, start_time} = doc.data();
        return date > now.date || (date === now.date && now.minutes < parseInt(start_time, 10) * 60 + 30);
      });
      const max = council ? 20 : 10;
      if (active.length + records.length > max) {
        throw new HttpsError('resource-exhausted', `Максимум ${max} активных записей.`);
      }
    }

    const refs = records.flatMap(({id}) => [db.doc(`bookings/${id}`), db.doc(`laundry_slots/${id}`)]);
    if ((await tx.getAll(...refs)).some(doc => doc.exists)) {
      throw new HttpsError('already-exists', 'Это время уже занято.');
    }
    records.forEach(({id, publicSlot}, i) => {
      tx.create(refs[i * 2], {
        ...publicSlot, booking_id: id, uid: targetUid, booked_by: actor,
        booked_by_role: actorData.role, booked_for_other: other, status: 'active',
        name: target.name, room: target.room, created_at: FieldValue.serverTimestamp()
      });
      tx.create(refs[i * 2 + 1], publicSlot);
    });
    // Serialize concurrent reservations by the same account so they share one limit.
    if (!admin) tx.update(actorRef, {last_laundry_booking_at: FieldValue.serverTimestamp()});
    return {count: records.length};
  });
});

exports.cancelLaundry = onCall(async request => {
  const actor = laundryActor(request);
  const id = request.data?.id;
  if (typeof id !== 'string' || !laundrySlotId.test(id)) {
    throw new HttpsError('invalid-argument', 'Неверная запись.');
  }
  const booking = db.doc(`bookings/${id}`);
  const slot = db.doc(`laundry_slots/${id}`);
  const history = db.collection('booking_history').doc();

  return db.runTransaction(async tx => {
    const snapshot = await tx.get(booking);
    if (!snapshot.exists) throw new HttpsError('not-found', 'Запись уже недоступна.');
    const data = snapshot.data();
    const profile = await tx.get(db.doc(`users/${actor}`));
    const role = profile.data()?.role;
    if ((await tx.get(db.doc(`deleted_users/${actor}`))).exists ||
        !(data.uid === actor || role === 'admin' || (role === 'student_council' && data.booked_by === actor))) {
      throw new HttpsError('permission-denied', 'Нет права отменить эту запись.');
    }
    if ((await tx.get(db.doc(`deleted_users/${data.uid}`))).exists) {
      throw new HttpsError('not-found', 'Запись уже недоступна.');
    }
    const createdAt = data.created_at;
    const revision = createdAt ? `${createdAt.seconds}:${createdAt.nanoseconds}` : null;
    if (request.data.revision !== revision) {
      throw new HttpsError('failed-precondition', 'Запись изменилась. Откройте её заново.');
    }

    tx.create(history, {
      uid: data.uid, booked_by: data.booked_by || data.uid, name: data.name || '',
      room: data.room || '', date: data.date, start_time: data.start_time,
      machine_number: data.machine_number, history_id: history.id, booking_id: id,
      status: 'cancelled', cancelled_by: actor, cancelled_at: FieldValue.serverTimestamp()
    });
    tx.delete(booking);
    tx.delete(slot);
    return {complete: true};
  });
});
