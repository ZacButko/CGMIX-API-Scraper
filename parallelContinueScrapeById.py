import logging
from threading import Thread, Lock
import pandas as pd
from pullData import getSummary
import json
from queue import Queue
from time import time
import math

# heavy use of code from this example : https://www.toptal.com/python/beginners-guide-to-concurrency-and-parallelism-in-python
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

nToRun = 220000
batch_size = 1000
num_threads = 10

failedIds = []
count = 0
summaryFile = "compiledData/compiledSummaryData.csv"
failedIdsFile = "failedIds.json"
with open(failedIdsFile) as f:
    failedIds = json.load(f)["failedIds"]
df = pd.read_csv(summaryFile, index_col=False)
ids = sorted(list(df["VesselId"]))
missingIds = sorted(set(range(ids[0], ids[-1])) - set(ids) - set(failedIds))
newFails = []
nToRun = min(nToRun, len(missingIds))


def worker(q, lock):
    while True:
        id = q.get()
        try:
            logging.debug(f"getting id {id}")
            res = getSummary({"vesselId": id})
        except:
            pass
        else:
            with lock:
                global df, failedIds, count, newFails
                if not res.empty:
                    df = df.append(res, ignore_index=True)
                    count += 1
                else:
                    failedIds.append(id)
                    newFails.append(id)
        finally:
            q.task_done()


if __name__ == "__main__":
    ts = time()
    tl = ts
    q = Queue()
    lock = Lock()
    print(f"start df size {len(df.index)}, getting {nToRun} on {num_threads} threads.")

    for i in range(num_threads):
        t = Thread(name=f"Thread{i+1}", target=worker, args=(q, lock))
        t.daemon = True
        t.start()

    iterations = math.floor(nToRun / batch_size)
    remainder = nToRun - (iterations * batch_size)
    # run batches, then save
    for i in range(iterations):
        for j in range(batch_size):
            q.put(missingIds[i * batch_size + j])
        q.join()
        df = df.sort_values(["VesselId"])
        df.to_csv(summaryFile, index=False)
        with open(failedIdsFile, "w") as saveFile:
            json.dump({"failedIds": failedIds}, saveFile)
        logging.info(f"Batch {i+1} done: {time() - tl:.2f} seconds")
        tl = time()

    # get remainder if there is any
    for j in range(remainder):
        q.put(missingIds[batch_size * iterations + j])
    q.join()
    # all workers completed. save final result
    df = df.sort_values(["VesselId"])
    df.to_csv(summaryFile, index=False)
    with open(failedIdsFile, "w") as saveFile:
        json.dump({"failedIds": failedIds}, saveFile)
    # print final stats
    print(f"main done, found {count} new entries")
    logging.debug(f"New fails: {newFails}.")
    print(f"end df size {len(df.index)}")
    logging.info(f"Total elapsed time: {time() - ts:.2f} seconds")
