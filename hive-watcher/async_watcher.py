from typing import Set
import json
import beem
import zmq.asyncio
import asyncio
from beem import blockchain
from beem.account import Account
from beem.blockchain import Blockchain
import uvloop
from timeit import default_timer as timer
import threading

WATCHED_OPERATION_IDS = ["podping", "hive-hydra"]

hive = beem.Hive()
blockchain = Blockchain(mode="head", blockchain_instance=hive)


class Pings:
    total_pings = 0
    latest_block = 0



def get_stream(block_num=None):
    """Open up a stream from Hive either live or history"""

    # If you want instant confirmation, you need to instantiate
    # class:beem.blockchain.Blockchain with mode="head",
    # otherwise, the call will wait until confirmed in an irreversible block.
    # noinspection PyTypeChecker
    global blockchain
    if block_num:
        # History
        stream = blockchain.stream(
            opNames=["custom_json"],
            start=block_num,
            max_batch_size=50,
            raw_ops=False,
            threading=False,
        )
    else:
        # Live
        stream = blockchain.stream(
            opNames=["custom_json"], raw_ops=False, threading=False
        )
    return stream


def get_allowed_accounts(acc_name="podping") -> Set[str]:
    """get a list of all accounts allowed to post by acc_name (podping)
    and only react to these accounts"""

    # This is giving an error if I don't specify api server exactly.
    # TODO reported as Issue on Beem library https://github.com/holgern/beem/issues/301
    h = beem.Hive(node="https://api.hive.blog")

    master_account = Account(acc_name, blockchain_instance=h, lazy=True)

    return set(master_account.get_following())


def allowed_op_id(operation_id) -> bool:
    """Checks if the operation_id is in the allowed list"""
    if operation_id in WATCHED_OPERATION_IDS:
        return True
    else:
        return False


async def get_url_from_blockchain(stream, url_q):
    """Fetchs one URL from the blockchain"""
    allowed_accounts = get_allowed_accounts()
    start_time = timer()
    for post in stream:
        if allowed_op_id(post["id"]):
            if set(post["required_posting_auths"]) & allowed_accounts:
                custom_data = json.loads(post.get("json"))
                data = {}
                data["required_posting_auths"] = post.get("required_posting_auths")
                data["trx_id"] = post.get("trx_id")
                data["timestamp"] = post.get("timestamp")
                data["block_num"] = post.get("block_num")
                if custom_data.get("url"):
                    await url_q.put((custom_data.get("url"),data))
                    print("--------------------------------")
                elif custom_data.get("urls"):
                    for url in custom_data.get("urls"):
                        await url_q.put((url, data))




async def print_url_loop(url_q):
    """Main loop fetching urls from hive and printing them"""

    async def get_from_queue():
        try:
            return await url_q.get()
        except RuntimeError:
            return

    while True:
        try:
            url, data = await asyncio.wait_for(
                get_from_queue(), timeout=0.01
            )
            if url:
                Pings.total_pings += 1
                Pings.latest_block = data.get('block_num')
                print(
                    f"Feed Updated - {data.get('timestamp')} - {data.get('trx_id')} "
                    f"- {url} - {data['required_posting_auths']}"
                )
        except asyncio.TimeoutError:
            pass
        except RuntimeError:
            return
        except Exception as ex:
            print(f"{ex} occurred")
        finally:
            # Always get the time of the loop
            # duration = timer() - start
            pass

def run(loop=None):
    if not loop:  # pragma: no cover
        uvloop.install()
        loop = asyncio.new_event_loop()

    live = get_stream()
    old = get_stream(54744709)
    url_q = asyncio.Queue(loop=loop)

    # Adding a Queue system for holding URLs and sending them out
    threading.Thread(target=get_url_from_blockchain(old,url_q), daemon=True).start()
    # threading.Thread(target=get_url_from_blockchain(live,url_q), daemon=True).start()

    loop.create_task(print_url_loop(url_q))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    run()
