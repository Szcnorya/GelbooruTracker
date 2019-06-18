import sqlite3
import requests
import concurrent.futures
import threading
from functools import partial
from bs4 import BeautifulSoup


class GelbooruMan:
    PageFetchLimit = 50

    def __init__(self):
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        self.localstore = threading.local()

    @staticmethod
    def PageUrl(url, pageNum):
        return url + '&pid={0}'.format(pageNum * 42)

    @staticmethod
    def SearchUrlByTag(tags):
        parentUrl = "https://gelbooru.com/index.php?page=post&s=list&tags={0}"
        return parentUrl.format(TagManager.serializeTag(tags))

    # HTML Parsing
    def IsLastPage(self, pageSoup):
        pagination = pageSoup.select('.pagination')[0].contents
        if pagination[-1].name != 'a':
            return True
        return False

    def ParseIdsFromPage(self, pageSoup):
        imglist = pageSoup.select('img.preview')
        imgIds = list(map(lambda x: x.parent['id'][1:], imglist))
        # imgHrefs = list(map(lambda x : 'https:' + x.parent['href'],imglist))
        return (imgIds, 0)

    def tagman(self):
        if not hasattr(self.localstore, "tagman"):
            self.localstore.tagman = TagManager()
        return self.localstore.tagman

    # UI funcs
    def checkUpdateThread(self, tag, page):
        r = requests.get(GelbooruMan.PageUrl(
            GelbooruMan.SearchUrlByTag(tag), page))
        soup = BeautifulSoup(r.text, 'html.parser')
        # Do something to the page
        imgIds, _ = self.ParseIdsFromPage(soup)
        newcnt = len(self.tagman().filterNewIds(tag, imgIds))
        return newcnt

    def checkUpdates(self):
        MarkTags = self.tagman().GetAllTags()
        summary = {}
        for MarkTag in MarkTags:
            tag_cnt = 0
            for page in range(0, GelbooruMan.PageFetchLimit,8):
                newcnts = list(self.pool.map(partial(self.checkUpdateThread,MarkTag),list(range(page,page+8))))
                tag_cnt += sum(newcnts)
                if(newcnts.count(0)!=0):
                    break
            summary[TagManager.serializeTag(MarkTag)] = tag_cnt
        # Report summary
        print("Updates summary:")
        for tag, newcnt in summary.items():
            tag = TagManager.deserializeTag(tag)
            print("Tag {0} has {1} new items, link: {2}".format(
                tag, newcnt, GelbooruMan.SearchUrlByTag(tag)))

    def commitThread(self, tag, page):
        r = requests.get(GelbooruMan.PageUrl(
                GelbooruMan.SearchUrlByTag(tag), page))
        soup = BeautifulSoup(r.text, 'html.parser')
        imgIds, _ = self.ParseIdsFromPage(soup)
        newIds = self.tagman().filterNewIds(tag, imgIds)
        return newIds

    def commitFromPid(self, tag, pidUB):
        if not self.tagman().IsExistTag(tag):
            return
        ToCommitIds = []
        for page in range(0, GelbooruMan.PageFetchLimit):
            newIds = self.commitThread(tag, page)
            nCnt = 0
            for id in newIds:
                if int(id) <= pidUB:
                    ToCommitIds.append(id)
                    nCnt += 1
            if(nCnt==0):
                break
        if(len(ToCommitIds)!=0):
            print("Collected {0} unseen pictures, start commit.".format(
                len(ToCommitIds)))
            self.tagman().commitIds(tag, ToCommitIds)
            print("Commit Finished.")
        else:
            print("No unseen pictures, abandon commit.")

    def commitFromPage(self, tag, page):
        if not self.tagman().IsExistTag(tag):
            return
        ToCommitIds = []
        for page in range(page, GelbooruMan.PageFetchLimit):
            newIds = self.commitThread(tag,page)
            ToCommitIds += newIds
            if(len(newIds)==0):
                break
        if(len(ToCommitIds)!=0):
            print("Collected {0} unseen pictures, start commit.".format(
                len(ToCommitIds)))
            self.tagman().commitIds(tag, ToCommitIds)
            print("Commit Finished.")
        else:
            print("No unseen pictures, abandon commit.")

    def SubscribeTag(self, tag: list):
        if self.tagman().IsExistTag(tag):
            print("Tag already exist.")
        else:
            self.tagman().AddTag(tag)
            print("Successsful subscribe tag {0}".format(tag))

    def UnsubsribeTag(self, tag: list):
        # Delete all tag_ids has this tag, and remove tag
        if not self.tagman().IsExistTag(conn, tag):
            print("Does not exist this tag.")
        else:
            self.tagman().deleteTag(tag)

    def ListTag(self):
        MarkTags = self.tagman().GetAllTags()
        print("Currently subscribe to {0} tags".format(len(MarkTags)))
        print(MarkTags)


class TagManager:

    def __init__(self):
        self.conn = self._CheckDBSanity()

    def _CheckDBSanity(self):
        conn = sqlite3.connect('tags.db')
        c = conn.cursor()
        c.execute(
            '''CREATE TABLE IF NOT EXISTS tags (tid INTEGER PRIMARY KEY AUTOINCREMENT, tag TEXT not null, UNIQUE (tag));''')
        c.execute('''CREATE TABLE IF NOT EXISTS tag_ids (tid INTEGER NOT NULL, pid INTEGER NOT NULL, PRIMARY KEY (tid,pid), FOREIGN KEY(tid) REFERENCES tags(tid));''')
        conn.commit()
        return conn

    def IsExistTag(self, tag):
        tag_s = TagManager.serializeTag(tag)
        c = self.conn.cursor()
        c.execute('''SELECT tag FROM tags WHERE tag='{0}'; '''.format(tag_s))
        t = c.fetchone()
        return t != None

    def GetAllTags(self):
        c = self.conn.cursor()
        c.execute('SELECT tag FROM tags')
        tags_s = c.fetchall()
        return list(map(lambda tag: TagManager.deserializeTag(tag[0]), tags_s))

    def AddTag(self, tag: list):
        tag_s = TagManager.serializeTag(tag)
        c = self.conn.cursor()
        c.execute('''INSERT INTO tags(tag) VALUES ('{0}');'''.format(tag_s))
        self.conn.commit()

    def getTagId(self, tag: list) -> int:
        tag_s = TagManager.serializeTag(tag)
        c = self.conn.cursor()
        c.execute(
            '''SELECT tid FROM tags WHERE tags.tag = '{0}';'''.format(tag_s))
        return c.fetchone()[0]

    def commitIds(self, tag: list, ids: list):
        tid = self.getTagId(tag)
        c = self.conn.cursor()
        c.execute('''INSERT INTO tag_ids VALUES ''' +
                  ",".join(list(map(lambda id: "({0},{1})".format(tid, id), ids))) + ';')
        self.conn.commit()

    def filterNewIds(self, tag: list, ids) -> int:
        tid = self.getTagId(tag)
        c = self.conn.cursor()
        c.execute(
            '''SELECT pid FROM tag_ids WHERE tag_ids.tid = {0};'''.format(tid))
        pids = c.fetchall()
        pids = [x[0] for x in pids]
        pset = set(pids)
        newids = []
        for id in ids:
            if int(id) not in pset:
                newids.append(id)
        return newids

    def deleteTag(self, tag: list):
        tid = self.getTagId(tag)
        c = self.conn.cursor()
        c.execute(
            '''DELETE FROM tag_ids WHERE tag_ids.tid = {0};'''.format(tid))
        c.execute('''DELETE FROM tags WHERE tags.tid = {0};'''.format(tid))
        self.conn.commit()

    @staticmethod
    def serializeTag(tag: list) -> str:
        return '+'.join(tag)

    @staticmethod
    def deserializeTag(tag: str) -> list:
        return tag.split('+')


def Help():
    print("""0-> checkTagUpdates(),
1-> ListTag, 2-> SubscribeTagX, 3-> UnsubscribeTagX,
4-> commitFromPageN,   5-> commitFromPidX,  6-> Help,
9-> Exit""")
if __name__ == '__main__':
    gelman = GelbooruMan()
    Dic = {0: gelman.checkUpdates, 1: gelman.ListTag, 2: gelman.SubscribeTag, 3: gelman.UnsubsribeTag,
           4: gelman.commitFromPage, 5: gelman.commitFromPid, 6: Help, 9: exit}
    Help()
    while True:
        Sel = int(input(">>"))
        if(4 <= Sel <= 5):
            Tag = input("Tag>>").split(" ")
            Num = int(input("PageNum or PID>>"))
            Dic[Sel](Tag, Num)
        elif (2 <= Sel <= 3):
            Tag = input("Tag>>").split(" ")
            Dic[Sel](Tag)
        else:
            Dic[Sel]()
