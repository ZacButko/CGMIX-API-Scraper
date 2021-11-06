import pandas as pd
from tqdm import tqdm
import time
import json
import aiohttp
import asyncio
import nest_asyncio
import math
import os.path

nest_asyncio.apply()
from functools import reduce
import logging
import xml.etree.ElementTree as ET

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

## load up request parameters and ids to run through
with open("cgmixConsts.json") as f:
    consts = json.load(f)
url = consts["url"]
xmlMethods = consts["xmlMethods"]
serviceTypeOptions = consts["serviceTypeOptions"]
metaFile = "cachedMetaData.json"


def getKnownIds():
    df = pd.read_csv("compiledData/compiledSummaryData.csv", index_col=False)
    return list(df["VesselId"])


def xmlToDF(xmlData, vesselId):
    try:
        root = ET.fromstring(xmlData)
        for i in range(4):
            root = root[0] if len(root) else {}
        if len(root) == 0:
            logging.debug(f"empty data for vesselId {vesselId}")
        outObjs = []
        for row in root:
            item = {}
            for field in row:
                # split is necessary to handle xml namespacing
                item[field.tag.split("}")[1]] = field.text

            if not "VesselId" in item:
                item["VesselId"] = vesselId
            outObjs.append(item)
        outDf = pd.DataFrame.from_records(outObjs)
        return outDf.astype({"VesselId": "int64"})
    except Exception as e:
        logging.debug(e)
        return pd.DataFrame()


async def aPostRequest(session, action, *bodyParams):
    body = xmlMethods[action]["body"].format(*bodyParams)
    try:
        async with session.post(url, data=body) as r:
            text = await r.text()
            text = text.replace("&lt;", "<")
            text = text.replace("&gt;", ">")
    except Exception as e:
        logging.debug(e)
        return pd.DataFrame()
        ## optional raise here
    else:
        return xmlToDF(text, bodyParams[0])


async def aGetManyXMLData(action, requestList):
    headers = {
        "content-type": "text/xml; charset=utf-8",
        "SOAPAction": xmlMethods[action]["action"],
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        rToGet = list(
            map(
                lambda *bodyParams: aPostRequest(session, action, *bodyParams),
                requestList,
            )
        )
        results = [
            await p for p in tqdm(asyncio.as_completed(rToGet), total=len(requestList))
        ]
        # return reduce(lambda a, b: a.append(b), results)
        return pd.concat(results)


saveFiles = {
    "particulars": "compiledData/compiledParticularsData.csv",
    "dimensions": "compiledData/compiledDimensionsData.csv",
    "tonnage": "compiledData/compiledTonnageData.csv",
}


def runNewBatches(df, batches, action, failedIds=[]):
    ## runs batches of scraping
    # df is dataframe you want to append to
    # batches is a list of ids to run
    # action is endpoint like 'dimensions' or 'tonnage'
    dfStartSize = len(df)
    with open(metaFile) as f:
        meta = json.load(f)
    for i, batch in enumerate(batches):
        logging.info(f"Starting batch {i+1}")
        res = asyncio.run(aGetManyXMLData(action, batch))
        if res.empty:
            failedIds = failedIds + batch
        else:
            df = df.append(res)
            df = df.sort_values(["VesselId"])
            df.to_csv(saveFiles[action], index=False)
            failedIds = failedIds + sorted(list(set(batch) - set(res["VesselId"])))
        with open(metaFile, "w") as f:
            meta["failedIds"][action] = failedIds
            json.dump(meta, f)
        logging.info(f"{len(res)} new rows fetched")

    logging.info(f"Total new rows added: {len(df) - dfStartSize}")
    logging.info("Save completed")


def continueScrape(action, N=0, batchSize=100000):
    saveFile = saveFiles[action]
    knownIds = getKnownIds()
    if os.path.exists(saveFile):
        # safely check previous savefile exists
        df = pd.read_csv(saveFile, index_col=False)
        fetchedIds = sorted(list(df["VesselId"]))
    else:
        df = pd.DataFrame()
        fetchedIds = []
    with open(metaFile) as f:
        meta = json.load(f)
        failedIds = [] if action not in meta["failedIds"] else meta["failedIds"][action]
    idsToRun = sorted(set(knownIds) - set(fetchedIds) - set(failedIds))
    # math to figure out what the batches should be
    if N == 0:
        N = len(idsToRun)
    if batchSize == 0:
        batchSize = N
    nToRun = min(N, len(idsToRun))
    iterations = math.floor(nToRun / batchSize)
    remainder = nToRun - (iterations * batchSize)
    batches = [idsToRun[i * batchSize : (i + 1) * batchSize] for i in range(iterations)]
    if remainder != 0:
        batches.append(
            idsToRun[iterations * batchSize : iterations * batchSize + remainder]
        )

    # everything's set to run
    logging.info(
        f"Pulling Data for {nToRun} vessels in batches of {batchSize}. {len(batches)} batches to run"
    )
    runNewBatches(df, batches, action, failedIds)


def getMissingIds(action):
    dataFile = saveFiles[action]
    df = pd.read_csv(dataFile, index_col=False)
    fetchedIds = sorted(list(df["VesselId"]))
    knownIds = getKnownIds()
    missingIds = sorted(set(knownIds) - set(fetchedIds))
    return missingIds


def continueParticularsScrape(*args):
    continueScrape("particulars", *args)


def continueDimensionsScrape(*args):
    continueScrape("dimensions", *args)


def continueTonnageScrape(*args):
    continueScrape("tonnage", *args)


def getFailedIds(action):
    with open(metaFile) as f:
        meta = json.load(f)
        if action in meta["failedIds"]:
            failedIds = meta["failedIds"][action]
        else:
            failedIds = []
    return failedIds


def clearFailedIds(action):
    with open(metaFile) as f:
        meta = json.load(f)
    with open(metaFile, "w") as f:
        meta["failedIds"][action] = []
        json.dump(meta, f)


def rerunFailedBatches(df, batches, action, allFails):
    ## reruns batches of failed ids
    # df is dataframe you want to append to
    # batches is a list of ids to run
    # action is endpoint like 'dimensions' or 'tonnage'
    dfStartSize = len(df)
    with open(metaFile) as f:
        meta = json.load(f)
    for i, batch in enumerate(batches):
        logging.info(f"Starting batch {i+1}")
        res = asyncio.run(aGetManyXMLData(action, batch))
        if not res.empty:
            df = df.append(res)
            df = df.sort_values(["VesselId"])
            df.to_csv(saveFiles[action], index=False)
            ## remove successfulIds from the failed ids list
            allFails = sorted(list(set(allFails) - set(res["VesselId"])))
            with open(metaFile, "w") as f:
                meta["failedIds"][action] = allFails
                json.dump(meta, f)
        logging.info(f"{len(res)} new rows fetched")

    logging.info(f"Total new rows added: {len(df) - dfStartSize}")
    logging.info("Save completed")


def rerunFailedIds(action, N=0, batchSize=100000):
    idsToRun = getFailedIds(action)
    if len(idsToRun) == 0:
        logging.info("Nothing to run")
    else:
        if os.path.exists(saveFiles[action]):
            # safely check previous savefile exists
            df = pd.read_csv(saveFiles[action], index_col=False)
        else:
            df = pd.DataFrame()
        # math to figure out what the batches should be
        if N == 0:
            N = len(idsToRun)
        if batchSize == 0:
            batchSize = N
        nToRun = min(N, len(idsToRun))
        iterations = math.floor(nToRun / batchSize)
        remainder = nToRun - (iterations * batchSize)
        batches = [
            idsToRun[i * batchSize : (i + 1) * batchSize] for i in range(iterations)
        ]
        if remainder != 0:
            batches.append(
                idsToRun[iterations * batchSize : iterations * batchSize + remainder]
            )

        # everything's set to run
        logging.info(
            f"Pulling Data for {nToRun} vessels in batches of {batchSize}. {len(batches)} batches to run"
        )
        rerunFailedBatches(df, batches, action, idsToRun)


def rerunAllFailed(batchSize=100000):
    for action in ["dimensions", "particulars", "tonnage"]:
        logging.info(f"Starting on {action}")
        rerunFailedIds(action, 0, batchSize)
        logging.info(f"{action.capitalize()} complete.")


def continueScrapeAll(batchSize=100000):
    for action in ["dimensions", "particulars", "tonnage"]:
        logging.info(f"Starting on {action}")
        continueScrape(action, 0, batchSize)
        logging.info(f"{action.capitalize()} complete.")


def pullDatabase(batchSize=100000):
    if os.path.exists(metaFile):
        with open(metaFile) as f:
            meta = json.load(f)
            if "scrapeStatus" not in meta:
                meta["scrapeStatus"] = "initialScrape"
            if "retriesCompleted" not in meta:
                meta["retriesCompleted"] = 0
            if "failedIds" not in meta:
                meta["failedIds"] = {}
    else:
        meta = {"scrapeStatus": "initialScrape", "retriesCompleted": 0, "failedIds": {}}

    if meta["scrapeStatus"] == "initialScrape":
        logging.info("Starting/continuing initial scrape.")
        continueScrapeAll(batchSize)
        meta["scrapeStatus"] = "retryFailedIds"
        with open(metaFile, "w") as f:
            json.dump(meta, f)
        logging.info("Initial scrape complete.")

    if meta["scrapeStatus"] == "retryFailedIds":
        nRetries = 2
        while meta["retriesCompleted"] < nRetries:
            logging.info(
                f'Starting to retry failed ids. Pass # {meta["retriesCompleted"] +1} of {nRetries}'
            )
            rerunAllFailed(batchSize)
            meta["retriesCompleted"] = meta["retriesCompleted"] + 1
            with open(metaFile, "w") as f:
                json.dump(meta, f)
            logging.info(f'Retry # {meta["retriesCompleted"]} complete.')

        meta["scrapeStatus"] = "complete"
        with open(metaFile, "w") as f:
            json.dump(meta, f)
        logging.info("All retries complete")

    if meta["scrapeStatus"] == "complete":
        logging.info("Congratulations you are done!")


if __name__ == "__main__":
    pullDatabase()
