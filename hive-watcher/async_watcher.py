from typing import Set
import json
import beem
import zmq.asyncio
import asyncio
from beem import blockchain
from beem.account import Account
from beem.blockchain import Blockchain
import uvloop

WATCHED_OPERATION_IDS = ["podping", "hive-hydra"]

hive = beem.Hive()
blockchain = Blockchain(mode="head", blockchain_instance=hive)


class Pings:
    total_pings = 0


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


async def get_url_from_blockchain():
    """Fetchs one URL from the blockchain"""
    stream = get_stream()
    allowed_accounts = get_allowed_accounts()
    for post in stream:
        if allowed_op_id(post["id"]):
            if set(post["required_posting_auths"]) & allowed_accounts:
                data = json.loads(post.get("json"))
                data["required_posting_auths"] = post.get("required_posting_auths")
                data["trx_id"] = post.get("trx_id")
                data["timestamp"] = post.get("timestamp")
                if data.get("url"):
                    yield (data.get("url"))
                elif data.get("urls"):
                    for url in data.get("urls"):
                        yield ((url, data))


async def print_url_loop():
    """Main loop fetching urls from hive and printing them"""
    async for url, data in get_url_from_blockchain():
        Pings.total_pings += 1
        print(
            f"Feed Updated - {data.get('timestamp')} - {data.get('trx_id')} "
            f"- {url} - {data['required_posting_auths']}"
        )


def run(loop=None):
    if not loop:  # pragma: no cover
        uvloop.install()
        loop = asyncio.new_event_loop()
    loop.create_task(print_url_loop())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    run()
