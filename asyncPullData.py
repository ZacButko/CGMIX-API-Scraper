### Core file to pull data from the CGMIX server asynchronously
# Data from the server is available via SOAP requests
# __main__ function runs pullDatabase which will agressivly and in a fail safe manner
# retrieve and compile data from all peripheral endpoints ['particulars', 'tonnage', 'dimensions']
# into respective csv 'tables' which should match the database format found on the target server.
# Async speedup is ~ 700 times faster than pulling endpoints one by one, and ~ 100 times faster than
# than multithreading synchronous requests.

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

saveFiles = {
    "summary": "compiledData/compiledSummaryData.csv",
    "particulars": "compiledData/compiledParticularsData.csv",
    "dimensions": "compiledData/compiledDimensionsData.csv",
    "tonnage": "compiledData/compiledTonnageData.csv",
    "meta": "cachedMetaData.json",
    "consts": "cgmixConsts.json",
}

with open(saveFiles["consts"]) as f:
    consts = json.load(f)
url = consts["url"]
xmlMethods = consts["xmlMethods"]
serviceTypeOptions = consts["serviceTypeOptions"]
metaFile = saveFiles["meta"]


def getKnownIds():
    # in compiledSummaryData we already fetched 'summary' endpoint data
    # Since the VesselId corresponds directly to the target database's
    # internal priary key, we scraped for all ids from 1 to 2 milion
    #
    # this function returns all active VesselIds we know about as a list
    df = pd.read_csv(saveFiles["summary"], index_col=False)
    return list(df["VesselId"])


def xmlToDF(xmlData, vesselId):
    # helper function to translate xml tree structured data into a pandas
    # dataframe
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
    # Basic async request
    # POSTS to 'url' with data of 'body'.
    # header information is in 'aGetManyXMLData' function
    body = xmlMethods[action]["body"].format(*bodyParams)
    try:
        async with session.post(url, data=body) as r:
            text = await r.text()
            text = text.replace("&lt;", "<")
            text = text.replace("&gt;", ">")
    except Exception as e:
        logging.debug(e)
        return pd.DataFrame()
        # TODO - respond appropriately to different types of connection errors
        # TODO - raise exception here
    else:
        return xmlToDF(text, bodyParams[0])


async def aGetManyXMLData(action, requestList):
    # Sets up an async session under which to run many async requests
    #   (this is much more efficient than opening a session for every request)
    # stores resulting dataframes in a list, then concats the list into one resultant
    # dataframe
    # tqdm is the progress bar :)
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
    # general idea is to pick up scraping where we left off
    # 1. grab a list of ids to run based on all ids we know about,
    #     minus ids that have failed and ids we already retreived
    # 2. create appropriate batches to run based on N ids to scrape for in baches of batchSize
    # 3. sends batches to runNewBatches()
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
    # helper function to check how many ids are still missing (need to be scrapped for)
    dataFile = saveFiles[action]
    df = pd.read_csv(dataFile, index_col=False)
    fetchedIds = sorted(list(df["VesselId"]))
    knownIds = getKnownIds()
    missingIds = sorted(set(knownIds) - set(fetchedIds))
    return missingIds


def getFailedIds(action):
    # helper function to check how many ids have failed (returned nothing or had an error)
    # for a given action (endpoint)
    with open(metaFile) as f:
        meta = json.load(f)
        if action in meta["failedIds"]:
            failedIds = meta["failedIds"][action]
        else:
            failedIds = []
    return failedIds


# def clearFailedIds(action):
#     # hacky way to rerun ids - just forget that they have failed
#     with open(metaFile) as f:
#         meta = json.load(f)
#     with open(metaFile, "w") as f:
#         meta["failedIds"][action] = []
#         json.dump(meta, f)


def continueParticularsScrape(*args):
    # Continue scrape of vessels on 'particulars' endpoint
    continueScrape("particulars", *args)


def continueDimensionsScrape(*args):
    # Continue scrape of vessels on 'dimensions' endpoint
    continueScrape("dimensions", *args)


def continueTonnageScrape(*args):
    # Continue scrape of vessels on 'tonnage' endpoint
    continueScrape("tonnage", *args)


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
    # bundles batches of failed ids to send to function 'rerunFailedBatches
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
    # checks all failed ids again
    for action in ["dimensions", "particulars", "tonnage"]:
        logging.info(f"Starting on {action}")
        rerunFailedIds(action, 0, batchSize)
        logging.info(f"{action.capitalize()} complete.")


def continueScrapeAll(batchSize=100000):
    # scrapes all endpoints for all known vessel ids
    for action in ["dimensions", "particulars", "tonnage"]:
        logging.info(f"Starting on {action}")
        continueScrape(action, 0, batchSize)
        logging.info(f"{action.capitalize()} complete.")


def pullDatabase(batchSize=100000):
    # Master function to load all perepheral endpoints based on a previously retreived 'summary' table.
    # Built to be incredibly fail-safe. Kill the process anytime and it will start again from
    #    its last known checkpoint (stored in 'metaFile')
    # 1. scrape all data on a first pass
    # 2. retry failed ids twice
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
