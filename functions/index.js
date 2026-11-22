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

  for (const collection of ['bookings', 'booking_history']) {
    await removeMatching(collection, 'uid', uid);
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
