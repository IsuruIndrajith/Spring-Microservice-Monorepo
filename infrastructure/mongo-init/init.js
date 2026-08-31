// Create product_db and an application user
const dbName = 'product_db';
const username = 'appuser';
const password = 'apppass';

db = db.getSiblingDB(dbName);
try {
  db.createUser({ user: username, pwd: password, roles: [{ role: 'readWrite', db: dbName }] });
  print('Created user ' + username + ' on ' + dbName);
} catch (e) {
  print('User creation error (maybe exists): ' + e);
}
