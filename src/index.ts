import * as Y from "yjs";
import * as promise from "lib0/promise";
import { ObservableV2 } from "lib0/observable";
import { openDatabaseSync, SQLiteDatabase } from "expo-sqlite";

export const PREFERRED_TRIM_SIZE = 500;

export class ExpoSQLitePersistence extends ObservableV2<{
  synced: (persistence: ExpoSQLitePersistence) => void;
  compacted: (persistence: ExpoSQLitePersistence, previousSize: number) => void;
}> {
  doc: Y.Doc;
  whenSynced: Promise<this>;
  db: SQLiteDatabase;
  synced: boolean = false;
  _dbsize = 0;
  _compacting = false;
  _destroyed = false;

  constructor(sqliteDatabase: SQLiteDatabase, doc: Y.Doc) {
    super();
    this.doc = doc;

    this.whenSynced = promise.create((resolve) =>
      this.on("synced", () => resolve(this))
    );

    this.db = sqliteDatabase;
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
          .getAllAsync("SELECT content FROM updates")
          .catch((e) => console.error("error loading updates", e))
      )
      .then((res) => {
        if (res != undefined && res.length) {
          doc.transact(() => {
            for (const row of res) {
              // @ts-ignore
              Y.applyUpdate(doc, new Uint8Array(row.content));
            }
          }, this);
          this._dbsize = res.length;
        }

        this.synced = true;
        this.emit("synced", [this]);

        this._compactIfNeeded();
      });

    this.destroy = this.destroy.bind(this);
    this._storeUpdate = this._storeUpdate.bind(this);
    doc.on("update", this._storeUpdate);
    doc.on("destroy", this.destroy);
  }
  _storeUpdate(update: Uint8Array, origin: any) {
    if (!this.db || origin == this) {
      !this.db && console.error("trying to store update without db");
      return;
    }

    this._dbsize++;
    this._compactIfNeeded();

    return this.db
      .runAsync(`INSERT INTO updates (content) VALUES (?)`, [update])
      .catch((e) => console.error("error storing update", e));
  }

  async _compactIfNeeded() {
    if (this._dbsize <= PREFERRED_TRIM_SIZE) return;
    if (this._destroyed) return;
    if (this._compacting) return;
    this._compacting = true;

    try {
      const stats = await this.db.getFirstAsync<{
        cnt: number;
        maxId: number;
      }>("SELECT COUNT(*) as cnt, MAX(id) as maxId FROM updates");
      if (!stats || stats.cnt <= PREFERRED_TRIM_SIZE) {
        this._dbsize = stats?.cnt ?? 0;
        return;
      }

      // NB: `compacted` might include data from updates after `maxId`, but
      // that's fine: Yjs updates are idempotent, so applying the compacted state
      // and then the newer updates will yield the same result.
      const compacted = Y.encodeStateAsUpdate(this.doc);
      let newSize = 1;
      await this.db.withExclusiveTransactionAsync(async (tx) => {
        await tx.runAsync("DELETE FROM updates WHERE id <= ?", [stats.maxId]);
        await tx.runAsync("INSERT INTO updates (content) VALUES (?)", [
          compacted,
        ]);
        const row = await tx.getFirstAsync<{ cnt: number }>(
          "SELECT COUNT(*) as cnt FROM updates"
        );
        if (row) newSize = row.cnt;
      });

      this._dbsize = newSize;
      this.emit("compacted", [this, stats.cnt]);
    } catch (e) {
      console.error("error compacting updates", e);
    } finally {
      this._compacting = false;
    }
  }

  destroy() {
    this.doc.off("update", this._storeUpdate);
    this.doc.off("destroy", this.destroy);
    this._destroyed = true;
    return this.db.closeSync();
  }
}
