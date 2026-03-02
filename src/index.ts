import * as Y from "yjs";
import * as promise from "lib0/promise";
import { ObservableV2 } from "lib0/observable";
import { openDatabaseSync, SQLiteDatabase } from "expo-sqlite";

export const PREFERRED_TRIM_SIZE = 500;

const DB_PREFIX = "exposqlite-persistence-";

export class ExpoSQLitePersistence extends ObservableV2<{
  synced: (persistence: ExpoSQLitePersistence) => void;
  compacted: (persistence: ExpoSQLitePersistence, previousSize: number) => void;
}> {
  doc: Y.Doc;
  name: string;
  whenSynced: Promise<this>;
  db: SQLiteDatabase;
  synced: boolean = false;
  _dbref = 0;
  _dbsize = 0;
  _destroyed = false;

  constructor(name: string, doc: Y.Doc) {
    super();
    this.doc = doc;
    this.name = name;

    this.whenSynced = promise.create((resolve) =>
      this.on("synced", () => resolve(this))
    );

    this.db = openDatabaseSync(`${DB_PREFIX}${name}.sqlite`);
    this.db
      .execAsync(
        "CREATE TABLE IF NOT EXISTS updates (id INTEGER PRIMARY KEY, content BLOB)"
      )
      .then(() => {
        const s = Y.encodeStateAsUpdate(doc);
        if (s.length > 0) {
          this._storeUpdate(s, null);
        }
      })
      .then(() =>
        this.db
          .getAllAsync("SELECT id, content FROM updates")
          .catch((e) => console.error("error loading updates", e))
      )
      .then((res) => {
        let maxLoadedId = 0;
        let loadedCount = 0;

        if (res != undefined && res.length) {
          for (const row of res) {
            // @ts-ignore
            Y.applyUpdate(doc, new Uint8Array(row.content));
            // @ts-ignore
            if (row.id > maxLoadedId) maxLoadedId = row.id;
          }
          loadedCount = res.length;
        }

        this.synced = true;
        this.emit("synced", [this]);

        if (loadedCount > PREFERRED_TRIM_SIZE) {
          this._compact(maxLoadedId, loadedCount);
        }
      });

    this.destroy = this.destroy.bind(this);
    this._storeUpdate.bind(this);
    doc.on("update", this._storeUpdate.bind(this));
    doc.on("destroy", this.destroy.bind(this));
  }
  _storeUpdate(update: Uint8Array, origin: any) {
    if (!this.db || origin == this) {
      !this.db && console.error("trying to store update without db");
      return;
    }

    return this.db
      .runAsync(`INSERT INTO updates (content) VALUES (?)`, [update])
      .catch((e) => console.error("error storing update", e));
  }

  _compact(maxLoadedId: number, loadedSize: number) {
    if (this._destroyed) return;

    // NB: `compacted` might include updated data after `maxLoadedId`, but
    // that's fine: Yjs updates are idempotent, so applying the compacted state
    // and then the updates will yield the same result as applying all updates
    // in order.
    const compacted = Y.encodeStateAsUpdate(this.doc);
    this.db
      .withExclusiveTransactionAsync(async (tx) => {
        await tx.runAsync("DELETE FROM updates WHERE id <= ?", [maxLoadedId]);
        await tx.runAsync("INSERT INTO updates (content) VALUES (?)", [
          compacted,
        ]);
      })
      .then(() => {
        this.emit("compacted", [this, loadedSize]);
      })
      .catch((e) => console.error("error compacting updates", e));
  }

  destroy() {
    this.doc.off("update", this._storeUpdate);
    this.doc.off("destroy", this.destroy);
    this._destroyed = true;
    return this.db.closeSync();
  }
}
