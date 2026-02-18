
# utils/db_monitor.py
import time
import logging

async def check_performance(db_storage):
    """
    Check MongoDB performance metrics (Phase 6).
    db_storage: Instance of MongoStorage
    """
    results = {}
    try:
        # 1. Ping Latency (Network/Server)
        start = time.perf_counter()
        await db_storage.db.command("ping")
        ping_ms = (time.perf_counter() - start) * 1000
        results['ping_ms'] = round(ping_ms, 2)
        
        # 2. Real Read Latency (Phase 5 Projection Check)
        start = time.perf_counter()
        # Direct check on users collection with projection
        await db_storage.users.find_one({"user_id": 0}, {"_id": 1})
        read_ms = (time.perf_counter() - start) * 1000
        results['read_ms'] = round(read_ms, 2)
        
        # 3. Cache Check (Internal metrics could be added here if needed)
        
        results['status'] = "Healthy" if read_ms < 15 else "Degraded"
        results['total_load'] = round(ping_ms + read_ms, 2)
        
        return results
        
    except Exception as e:
        logging.error(f"❌ DB Monitoring Error: {e}")
        return {"status": "Critical", "error": str(e)}

async def get_performance_report(db_storage):
    """Generate a human-readable performance report"""
    metrics = await check_performance(db_storage)
    
    report = (
        "📊 **Database Performance Report**\n\n"
        f"🔹 **Ping Latency:** `{metrics.get('ping_ms', 'N/A')}ms`\n"
        f"🔹 **Read Latency:** `{metrics.get('read_ms', 'N/A')}ms` (Target: <5ms)\n"
        f"🔹 **System Status:** `{'✅' if metrics.get('status') == 'Healthy' else '⚠️'} {metrics.get('status')}`\n"
    )
    
    if metrics.get('read_ms', 99) > 10:
        report += "\n> [!WARNING]\n> Read latency is high. Verify Atlas Region & Indexes."
        
    return report
