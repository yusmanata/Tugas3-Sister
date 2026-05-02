import argparse
import asyncio
import sys
import logging
from src.nodes.lock_manager import LockManager
from src.nodes.queue_node import QueueNode
from src.nodes.cache_node import CacheNode

async def main():
    parser = argparse.ArgumentParser(description="Distributed Sync System Node")
    parser.add_argument("--node-id", required=True, help="The ID of this node (e.g., node_1)")
    parser.add_argument("--service", required=True, choices=['lock', 'queue', 'cache'], help="Which service to run")
    
    args = parser.parse_args()
    
    # Enable info logging for main
    logging.basicConfig(level=logging.INFO, format=f'%(asctime)s - [{args.node_id}] - %(levelname)s - %(message)s')
    logger = logging.getLogger("Main")
    
    logger.info(f"Starting {args.service} service on {args.node_id}...")
    
    from src.utils.config import get_cluster_config
    cluster_config = get_cluster_config(args.service)
    
    node = None
    if args.service == 'lock':
        node = LockManager(args.node_id, cluster_config)
    elif args.service == 'queue':
        node = QueueNode(args.node_id, cluster_config)
    elif args.service == 'cache':
        node = CacheNode(args.node_id, cluster_config)
        
    if not node:
        logger.error("Failed to initialize node.")
        sys.exit(1)
        
    await node.start()
    logger.info("Service started successfully. Press Ctrl+C to exit.")
    
    # Keep the program running
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Stopping service...")
        await node.stop()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Program terminated by user.")
