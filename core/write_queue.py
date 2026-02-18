
# core/write_queue.py
import asyncio
from collections import defaultdict
from pymongo import UpdateOne
import logging

class WriteQueue:
    def __init__(self, db, flush_interval=5):
        self.db = db
        self.flush_interval = flush_interval
        # Using a tuple as key: (collection, filter_json, field)
        self._stat_increments = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()
        self._task = None

    def start(self):
        """Start the background flush loop"""
        if self._task is None:
            self._task = asyncio.create_task(self._flush_loop())
            logging.info(f"🚀 WriteQueue started (flush interval: {self.flush_interval}s)")

    async def increment_stat(self, collection, filter_doc, field, amount=1):
        """Buffer a stat increment"""
        # We need a hashable key for the filter dict
        filter_key = str(sorted(filter_doc.items()))
        key = (collection, filter_key, field)
        
        async with self._lock:
            self._stat_increments[key]['amount'] += amount
            self._stat_increments[key]['filter'] = filter_doc
            self._stat_increments[key]['collection'] = collection

    async def _flush_loop(self):
        while True:
            try:
                await asyncio.sleep(self.flush_interval)
                await self._flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"⚠️ WriteQueue loop error: {e}")

    async def _flush(self):
        async with self._lock:
            if not self._stat_increments:
                return
            snapshot = dict(self._stat_increments)
            self._stat_increments.clear()

        # Group operations by collection
        by_collection = defaultdict(list)
        for key, data in snapshot.items():
            coll_name = data['collection']
            field_name = key[2]
            op = UpdateOne(
                data['filter'],
                {'$inc': {field_name: data['amount']}},
                upsert=True
            )
            by_collection[coll_name].append(op)

        # Execute bulk writes for each collection
        for coll_name, ops in by_collection.items():
            try:
                coll = getattr(self.db, coll_name)
                # ordered=False allows individual writes to succeed even if others fail
                await coll.bulk_write(ops, ordered=False)
                logging.debug(f"✅ WriteQueue flushed {len(ops)} ops to {coll_name}")
            except Exception as e:
                logging.error(f"❌ WriteQueue flush error [{coll_name}]: {e}")
