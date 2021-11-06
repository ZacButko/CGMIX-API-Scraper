import requests
import xml.etree.ElementTree as ET
import pandas as pd
import os
import time
from consts import countries
import glob
from tqdm import tqdm
import json

verbose = False
cacheTempData = False

with open("cgmixConsts.json") as f:
    consts = json.load(f)
url = consts["url"]
xmlMethods = consts["xmlMethods"]
serviceTypeOptions = consts["serviceTypeOptions"]

## verbose printing
def printV(*args):
    if verbose:
        print(*args)


# necessary to prevent bad path names
def toCamelCase(str):
    return "".join([t.title() for t in str.split()]).replace("/", "")


# pulls data from CGMIX API
def getXMLData(action, *bodyParams):
    headers = {
        "content-type": "text/xml; charset=utf-8",
        "SOAPAction": xmlMethods[action]["action"],
    }
    body = xmlMethods[action]["body"].format(*bodyParams)
    try:
        r = requests.post(url, data=body, headers=headers)
        r.encoding = "utf-8"
        text = r.text
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        return text
    except Exception as e:
        print(e)
        raise


def saveTempData(data, action):
    if not os.path.exists("tempData"):
        os.mkdir("tempData")
    with open(f"tempData/{action}TempData.xml", "w") as file:
        file.write(data)


def getRowData(xmlDataSet):
    outObjs = []
    for row in xmlDataSet:
        item = {}
        for field in row:
            # split is necessary to handle xml namespacing
            item[field.tag.split("}")[1]] = field.text
        outObjs.append(item)
    return outObjs


def findDataSet(root, endpointTime, action, *params):
    # if data exists, its 4 levels deep in this tree
    # not fully returned data (i.e. only 2 levels) can be treated
    # as empty data
    tempRoot = root
    for i in range(4):
        tempRoot = tempRoot[0] if len(tempRoot) else {}
    if len(tempRoot) == 0:
        printV(f"empty data set {endpointTime:.2f} sec", action, *params)
    return tempRoot


def getCgmixEndpoint(action, *params):
    t = time.perf_counter()
    try:
        xmlData = getXMLData(action, *params)
    except:
        raise
    if cacheTempData:
        saveTempData(xmlData, action)
    endpointTime = time.perf_counter() - t
    root = ET.fromstring(xmlData)
    # where the data is located
    newDataSet = findDataSet(root, endpointTime, action, *params)
    dimensions = getRowData(newDataSet)
    return pd.DataFrame.from_records(dimensions)


def getDimensions(vesselId):
    return getCgmixEndpoint("dimensions", vesselId)


def getOperation(activityId):
    return getCgmixEndpoint("operationControls", activityId)


def getCases(vesselId):
    return getCgmixEndpoint("cases", vesselId)


def getDeficiencies(activityId):
    return getCgmixEndpoint("deficiencies", activityId)


def getDocuments(vesselId):
    return getCgmixEndpoint("documents", vesselId)


def getParticulars(vesselId):
    return getCgmixEndpoint("particulars", vesselId)


def getSummary(p):
    df = getCgmixEndpoint(
        "summary",
        p.get("vesselId", ""),
        p.get("vesselName", ""),
        p.get("callSign", ""),
        p.get("vin", ""),
        p.get("hin", ""),
        p.get("flag", ""),
        p.get("service", ""),
        p.get("buildYear", ""),
    )
    if df.empty:
        return df
    else:
        return df.astype({"VesselId": "int64", "ConstructionCompletedYear": "int32"})


def getTonnage(vesselId):
    return getCgmixEndpoint("tonnage", vesselId)


def scrapeYear(year):
    t = time.perf_counter()
    df = getSummary({"buildYear": year})
    elapsed = time.perf_counter() - t
    if df.empty and elapsed > 10:
        printV("Data set was empty, but probably data here... Trying again")
        results = 0
        for service in serviceTypeOptions:
            t = time.perf_counter()
            df = getSummary({"buildYear": year, "service": service})
            elapsed = time.perf_counter() - t
            if df.empty and elapsed > 10:
                printV(f"Probably too much data! Year: {year}, Service: {service}")
                found = False
                for country in countries:
                    t = time.perf_counter()
                    df = getSummary(
                        {"buildYear": year, "service": service, "flag": country}
                    )
                    elapsed = time.perf_counter() - t
                    if df.empty and elapsed > 10:
                        printV(
                            f"Probably too much data! Year: {year}, Service: {service}, Country: {country}"
                        )
                    else:
                        found = True
                        df.to_csv(
                            f"scrapedData/{year}-{toCamelCase(service)}-{toCamelCase(country)}.csv"
                        )
                if found:
                    results += 1
            else:
                df.to_csv(f"scrapedData/{year}-{toCamelCase(service)}.csv")
                results += 1
        printV(
            "Nothing found"
            if results == 0
            else f"{results} service types found for {year}"
        )
    else:
        df.to_csv(f"scrapedData/{year}.csv")


def scrapeSummaryByYear(yearStart, yearEnd=None):
    if not os.path.exists("scrapedData"):
        os.mkdir("scrapedData")

    if yearEnd:
        for year in range(yearStart, yearEnd + 1):
            scrapeYear(year)
    else:
        scrapeYear(yearStart)


def compileData():
    df = pd.DataFrame
    for i, csv in enumerate(glob.glob("scrapedData/*.csv")):
        if i == 0:
            df = pd.read_csv(csv)
        else:
            df2 = pd.read_csv(csv)
            df = df.append(df2, ignore_index=True)

    df.to_csv("compiledData/compiledData.csv")


def getMissingIds():
    summaryFile = "compiledData/compiledSummaryData.csv"
    df = pd.read_csv(summaryFile, index_col=False)
    ids = sorted(list(df["VesselId"]))
    missingIds = sorted(set(range(ids[0], ids[-1])) - set(ids))
    return missingIds


def getRemainingIdsToCheck():
    summaryFile = "compiledData/compiledSummaryData.csv"
    failedIdsFile = "failedIds.json"
    with open(failedIdsFile) as f:
        failedIds = json.load(f)["failedIds"]
    df = pd.read_csv(summaryFile, index_col=False)
    ids = sorted(list(df["VesselId"]))
    missingIds = sorted(set(range(ids[0], ids[-1])) - set(ids) - set(failedIds))
    return missingIds


def continueSummaryScrapeById(nToRun):
    summaryFile = "compiledData/compiledSummaryData.csv"
    failedIdsFile = "failedIds.json"
    with open(failedIdsFile) as f:
        failedIds = json.load(f)["failedIds"]

    df = pd.read_csv(summaryFile, index_col=False)
    ids = sorted(list(df["VesselId"]))
    missingIds = sorted(set(range(ids[0], ids[-1])) - set(ids) - set(failedIds))

    count = 0
    for i in tqdm(range(nToRun)):
        try:
            res = getSummary({"vesselId": missingIds[i]})
        except:
            pass
        else:
            if not res.empty:
                df = df.append(res, ignore_index=True)
                count += 1
            else:
                failedIds.append(missingIds[i])

        if i != 0 and i % 100 == 0:
            df = df.sort_values(["VesselId"])
            df.to_csv(summaryFile, index=False)
            with open(failedIdsFile, "w") as saveFile:
                json.dump({"failedIds": failedIds}, saveFile)

    # save updated data set
    df = df.sort_values(["VesselId"])
    df.to_csv(summaryFile, index=False)
    # saved updated roster of failed ids
    with open(failedIdsFile, "w") as saveFile:
        json.dump({"failedIds": failedIds}, saveFile)
    print(f"Added {count} rows. Save complete")
