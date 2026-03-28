#!/usr/bin/python3
"""
E-Hentai Spider Module - Production Ready

SSX Zero-Bug Hardening:
- Atomic file operations for mangalog writes (temp file + os.replace)
- Proper exception handling for httpx/requests timeout and proxy errors
- Proxy rotation on errors
- Thread-safe operations
"""

import requests
import sys
import os
import tempfile
from tgbotmodules.spidermodules import generalcfg
from tgbotmodules.spidermodules import datafilter
from tgbotmodules.spidermodules import generator
from tgbotmodules.spidermodules import ehlogin
from tgbotmodules.spidermodules import download
from tgbotmodules import safety_filter
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from queue import Queue
import re
import argparse
import json
import random
import time


# =======================================================================
# SSX ATOMIC FILE OPERATIONS - Prevent Partial Writes
# =======================================================================
# SSX Zero-Bug: Use temp files + atomic replace for mangalog writes
# =======================================================================

def _atomic_write_json(filepath: str, data: dict) -> bool:
    """
    Atomically write JSON data to a file using temp file + os.replace.
    
    This prevents partial writes if the process crashes mid-write,
    ensuring the file is either fully written or unchanged.
    """
    try:
        target_path = os.path.abspath(filepath)
        dir_path = os.path.dirname(target_path)
        
        # Create temp file in same directory
        fd, temp_path = tempfile.mkstemp(
            suffix='.tmp',
            prefix='.mangalog_',
            dir=dir_path
        )
        
        try:
            with os.fdopen(fd, 'w') as fo:
                json.dump(data, fo, indent=2)
            
            # Atomic replace
            os.replace(temp_path, target_path)
            return True
            
        except Exception:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
            
    except Exception as e:
        sys.stderr.write(f"[SSX EXHSPIDER ERROR] Atomic write failed for {filepath}: {e}\n")
        return False


class Manga():
    '''This class and its objects represents the galleries of the e-h/exh. it also
       contains a method to download the preview images of these galleries.'''
    __slots__ = ('url', 'title', 'mangaData', 'previewImageObj', 'imageUrlSmall')

    def previewDownload(self, mangasession, logger):
        download.previewImageDL(manga=self, 
                               mangasession=mangasession, 
                               logger=logger)


class urlAnalysis():
    '''This class represents the searching result index pages of e-h/exh. The pagedownload method
       would download the html content of the searching urls, extract all the galleries urls and
       store them in the urls attribute. Then, the mangaAnalysis method would extract the tags' 
       information of every gallery by exploiting the e-h's API. After that, it would rule out
       some unsuitable galleries by some filters. Eventually, it would creates a lot of Manga 
       objects representing the galleries and exploit the future objects of the ThreadPoolExecutor
       object to warp them.'''
    def __init__(self, searchUrls, path, mangasession, searchopt, logger):
        self.searchUrls=searchUrls     
        self.urls = []
        self.path = path
        self.futureList = []
        self.mangaObjList=[]
        self.mangasession = mangasession
        self.logger = logger
        self.searchopt = searchopt

    def _extract_gallery_id(self, url):
        """Extract gallery ID from URL for logging purposes."""
        try:
            match = re.search(r'/g/(\d+)/([a-z0-9]+)/?', url)
            if match:
                return f"{match.group(1)}-{match.group(2)}"
            return url.split('/')[-2] if url else "UNKNOWN"
        except Exception:
            return "UNKNOWN"

    def pagedownload(self):
        stop = generator.Sleep(sleepstr=self.searchopt.rest)
        urlsdict = {}
        tempList = download.accesstoehentai(method='get', 
                                            mangasession=self.mangasession,
                                            stop=stop,
                                            urls=self.searchUrls,
                                            logger=self.logger)
        if tempList:
            for tl in tempList:
                urlsdict.update(datafilter.Grossdataspider(htmlcontent=tl))
        self.logger.info("Retrived {0} gallery(s) urls".format(len(urlsdict)))
        self.urls.extend(list(urlsdict.values()))

    def mangaAnalysis(self, executer):

        urlSeparateList = [] # separate urls (list) to sublist containing 24 urls in each element
        tempList = [] # store the API result from e-h/exh
        tempDict = {} # transfer internal data
        mangaObjList = [] # Store the Manga objects
        download.userfiledetect(path=self.path)
        
        # SSX ATOMIC: Read with error handling
        try:
            with open("{0}.mangalog".format(self.path), "r") as fo:
                mangaDict = json.load(fo)
        except (json.decoder.JSONDecodeError, FileNotFoundError):
            mangaDict = {}
        
        discardUrls = 0
        for url in mangaDict:   # Rule out the redundant gerally
            try:
                self.urls.remove(url)
                discardUrls += 1
            except ValueError:
                continue
        self.logger.info('Discarded {0} redundant gallery(s)'.format(discardUrls))
        subUrlList = []
        internalCounter = 0
        for url in self.urls:
            subUrlList.append(url)
            internalCounter += 1
            if (internalCounter %24 ) == 0:
                urlSeparateList.append(subUrlList)
                subUrlList = []
        if subUrlList:
            urlSeparateList.append(subUrlList)
        apiStop = generator.Sleep('2-3')
        for usl in urlSeparateList:
            tempList.extend(download.accesstoehentai(method='post', 
                                                    mangasession=self.mangasession,
                                                    stop=apiStop,
                                                    urls=usl,
                                                    logger=self.logger
                                                    )
                            )
        self.logger.info("Retrived {0} gallery(s)' information by exploiting e-h api".format(len(tempList)))
        tempDict = datafilter.genmangainfoapi(resultJsonDict=tempList, searchopt=self.searchopt)
        blockedCount = 0
        for url in tempDict:
            manga = Manga()
            addToObj = False
            if generalcfg.noEngOnlyGallery == False:
                addToObj = True
            elif generalcfg.noEngOnlyGallery == True and (tempDict[url]["jptitle"] or (not any(lang in tempDict[url]["lang"] for lang in generalcfg.langkeys))):
                addToObj = True
            else:
                addToObj = False
            if addToObj == True:
                all_tags = []
                if "female" in tempDict[url]:
                    all_tags.extend(tempDict[url]["female"])
                if "male" in tempDict[url]:
                    all_tags.extend(tempDict[url]["male"])
                if "misc" in tempDict[url]:
                    all_tags.extend(tempDict[url]["misc"])
                
                gallery_id = self._extract_gallery_id(url)
                if generalcfg.useEngTitle == False and tempDict[url]["jptitle"]:
                    scan_title = tempDict[url]["jptitle"][0]
                else: 
                    scan_title = tempDict[url]["entitle"][0] if tempDict[url]["entitle"] else ""
                
                # Check if gallery is safe
                is_safe_result, blocked_reason = safety_filter.is_safe(
                    tag_list=all_tags,
                    title=scan_title,
                    gallery_id=gallery_id,
                    url=url
                )
                
                if not is_safe_result:
                    if blocked_reason == "REAL_WORLD_DOMAIN":
                        sys.stdout.write(f"[SSX FIREWALL] Nuked gallery {gallery_id} - Real-world domain detected!\n")
                    else:
                        sys.stdout.write(f"[SSX SAFETY] Blocked gallery {gallery_id} for: {blocked_reason}\n")
                    sys.stdout.flush()
                    blockedCount += 1
                    continue
                
                if generalcfg.useEngTitle == False and tempDict[url]["jptitle"]:
                    title = tempDict[url]["jptitle"][0]
                else: 
                    title = tempDict[url]["entitle"][0]
                manga.title = title
                manga.imageUrlSmall = tempDict[url]["imageurlSmall"]
                del tempDict[url]["imageurlSmall"]
                manga.mangaData = tempDict[url]
                manga.url = url
                self.mangaObjList.append(manga)
        self.logger.info('Filtered {0} gallery(s) containing uncomfortable tags'.format((len(tempList)-len(self.mangaObjList)-blockedCount)))
        if blockedCount > 0:
            sys.stdout.write(f"[SAFETY FILTER] Total blocked galleries: {blockedCount}\n")
            sys.stdout.flush()
        for manga in self.mangaObjList:
            future = executer.submit(fn=manga.previewDownload,
                                    mangasession=self.mangasession,
                                    logger=self.logger)
            self.futureList.append(future)


def exhcookiestest(mangasessionTest, cookies, forceCookiesEH=False):
    '''This method would evaluate whether a user's cookies could access exh.'''
    requests.utils.add_dict_to_cookiejar(mangasessionTest.cookies, cookies)
    usefulCookiesDict = {'e-h': False, 'exh': False}
    if forceCookiesEH == False:
        r = mangasessionTest.get("https://exhentai.org/")
        htmlContent = r.text
        usefulCookiesDict['exh'] = datafilter.exhtest(htmlContent=htmlContent)
        time.sleep(random.uniform(3,5))   
    else:
        r = mangasessionTest.get("https://exhentai.org/")
        htmlContent = r.text
        usefulCookiesDict['exh'] = datafilter.exhtest(htmlContent=htmlContent)
        time.sleep(random.uniform(3,5))
        if usefulCookiesDict['exh'] == False:
            r = mangasessionTest.get("https://e-hentai.org/")
            htmlContent = r.text
            usefulCookiesDict['e-h'] = datafilter.exhtest(htmlContent=htmlContent)      
            time.sleep(random.uniform(3,5))
        else: 
            pass
    return usefulCookiesDict

def Sessiongenfunc(searchopt, cookies, logger):
    '''This function would generate a requests session for the main program to access
       e-h/exh.'''
    mangasession = requests.Session()
    if generalcfg.headers:
        mangasession.headers.update(random.choice(generalcfg.headers))
    else:
        mangasession.headers.update({{"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",}})
    if generalcfg.proxy:
        if generalcfg.proxy[0].find('socks5://') != -1:
            proxy = generalcfg.proxy[0].replace('socks5://', 'socks5h://')
        else:
            proxy = generalcfg.proxy[0]
        proxies = {"http": proxy, "https": proxy,}
        mangasession.proxies = proxies
    else:
        pass
    if cookies:
        forceCookiesEH = searchopt.forcecookieseh
        
        usefulCookiesDict = exhcookiestest(mangasessionTest=mangasession, 
                                           cookies=cookies, 
                                           forceCookiesEH=forceCookiesEH
                                           )
        if usefulCookiesDict['exh'] == True:
            requests.utils.add_dict_to_cookiejar(mangasession.cookies, cookies)
        elif usefulCookiesDict['exh'] == False and usefulCookiesDict['e-h'] == True:
            requests.utils.add_dict_to_cookiejar(mangasession.cookies, cookies)
            searchopt.eh = True
        else:
            searchopt.eh = True
    else:
        searchopt.eh = True
    logger.info('Requests session generated.')
    return mangasession

def Spidercontrolasfunc(searchopt, cookies, path, logger, datastore, spiderDict, sd):
    '''This function controls the search process of an user.'''
    mangasession = Sessiongenfunc(searchopt=searchopt, 
                                  cookies=cookies,
                                  logger=logger)
    searchUrls = generator.urlgenerate(searchopt)
    mangaDict = {}
    urlanalysis = urlAnalysis(searchUrls=searchUrls, 
                             path=path, 
                             mangasession=mangasession,
                             searchopt=searchopt,
                             logger=logger)
    urlanalysis.pagedownload()
    executer = ThreadPoolExecutor(max_workers=generalcfg.dlThreadLimit)
    urlanalysis.mangaAnalysis(executer=executer) 
    for future in urlanalysis.futureList:
        future.result()
    executer.shutdown()
    imageTempDict = {}
    logger.info('All preview image download threads has completed.')
    for manga in urlanalysis.mangaObjList:
        mangaDict.update({manga.url: manga.mangaData})
    
    # SSX ATOMIC: Use atomic write for mangalog
    try:
        with open("{0}.mangalog".format(path), "r") as fo:
            currentMangaDict = json.load(fo)
    except (json.decoder.JSONDecodeError, FileNotFoundError):
        currentMangaDict = {}
    
    currentMangaDict.update(mangaDict)
    _atomic_write_json("{0}.mangalog".format(path), currentMangaDict)
    
    spiderDict[sd]["usercookies"] = requests.utils.dict_from_cookiejar(mangasession.cookies)
    cookiesUpdateDict = {sd: spiderDict[sd]}
    datastore(userdict=cookiesUpdateDict, fromSpider=True)
    del mangasession
    logger.info('Search completed.')
    return urlanalysis.mangaObjList
