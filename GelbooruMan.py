import gevent
from gevent import monkey; monkey.patch_all()
import sqlite3
import requests
import threading
from functools import partial
from bs4 import BeautifulSoup
import itertools

# import time
# import logging
# def profile(func):
#     def wrap(*args, **kwargs):
#         started_at = time.time()
#         result = func(*args, **kwargs)
#         logging.debug("Function %s: %f",func.__name__,time.time() - started_at)
#         return result

#     return wrap

class GelbooruMan:
    PageFetchLimit = 50
    TagThreadSpike = 32
    PageThreadSpike = 2

    def __init__(self):
        self.localstore = threading.local()
        self.sess = requests.Session()
        self.sess.mount('https://',requests.adapters.HTTPAdapter(pool_connections=64,pool_maxsize=1024))

    @staticmethod
    def PageUrl(url, pageNum):
        """Append the Url with page argument."""
        return url + '&pid={0}'.format(pageNum * 42)

    @staticmethod
    def SearchUrlByTag(tags):
        """Make up a search query by tags('seperated as list')."""
        parentUrl = "https://gelbooru.com/index.php?page=post&s=list&tags={0}"
        return parentUrl.format(TagManager.SerializeTag(tags))

    @staticmethod
    def IsLastPage(pageSoup):
        """Return whether this page is the last page."""
        pagination = pageSoup.select('.pagination')[0].contents
        if pagination[-1].name != 'a':
            return True
        return False

    @staticmethod
    def ParseIdsFromPage(pageSoup) -> list:
        """Function for parsing image ids from Search page."""
        imglist = pageSoup.select('img.preview')
        imgIds = list(map(lambda x: int(x.parent['id'][1:]), imglist))
        # imgHrefs = list(map(lambda x : 'https:' + x.parent['href'],imglist))
        return (imgIds, 0)

    def tagman(self):
        """Member function for access thread-local Tag DB."""
        if not hasattr(self.localstore, "tagman"):
            self.localstore.tagman = TagManager()
        return self.localstore.tagman

    def session(self):
        # if not hasattr(self.localstore, "session"):
        #     self.localstore.session = requests.Session()
        # return self.localstore.session
        return self.sess

    def peekTagPagePid(self,tag:list,page:int) -> int:
        r = self.session().get(GelbooruMan.PageUrl(
            GelbooruMan.SearchUrlByTag(tag), page))
        soup = BeautifulSoup(r.text, 'html.parser')
        pids, _ = GelbooruMan.ParseIdsFromPage(soup)
        pid = pids[0]
        for id in pids:
            if pid < id:
                pid = id
        return pid

    def updateTagMan(self):
        MarkTags = self.tagman().GetAllTags()
        for cur in range(0,len(MarkTags),GelbooruMan.TagThreadSpike):
            tags = MarkTags[cur:cur+GelbooruMan.TagThreadSpike]
            # tags_ids = list(self.pool.map(self.updateTagThread,tags))
            jobs = [gevent.spawn(self.updateTagThread, tag) for tag in tags]
            gevent.joinall(jobs,timeout=5)
            tags_ids = [job.value for job in jobs]
            for tag,tag_ids in zip(tags,tags_ids):
                if(tag_ids!=None and len(tag_ids)!=0):
                    self.tagman().AddUncommitedIds(tag,tag_ids)

    def updateTagMan1Tag(self,tag):
        tag_ids =self.updateTagThread(tag)
        if(len(tag_ids)!=0):
            self.tagman().AddUncommitedIds(tag,tag_ids)

    def updateTagThread(self,tag : list) -> list:
        NewIds = []
        for page in range(0, GelbooruMan.PageFetchLimit,GelbooruMan.PageThreadSpike):
            # newPageIds = list(self.pool.map(partial(self.updateTagPageThread,tag),list(range(page,page+GelbooruMan.PageThreadSpike))))
            jobs = [gevent.spawn(self.updateTagPageThread,tag,p) for p in range(page,page+GelbooruMan.PageThreadSpike)]
            gevent.joinall(jobs,timeout=5)
            newPageIds = [job.value for job in jobs]
            for ids in newPageIds:
                if ids is not None:
                    NewIds += ids
            if(list(map(len,newPageIds)).count(0)!=0):
                break
        return NewIds

    def updateTagPageThread(self,tag:list, page: int) -> list:
        r = self.session().get(GelbooruMan.PageUrl(
            GelbooruMan.SearchUrlByTag(tag), page))
        soup = BeautifulSoup(r.text, 'html.parser')
        imgIds, _ = GelbooruMan.ParseIdsFromPage(soup)
        newIds = self.tagman().FilterNewIds(tag, imgIds)
        return newIds

    # UI funcs
    def checkUpdates(self):
        self.updateTagMan()
        MarkTags = self.tagman().GetAllTags()
        summary = {}
        for MarkTag in MarkTags:
            summary[TagManager.SerializeTag(MarkTag)] = len(self.tagman().GetAllUncommitedIds(MarkTag))
        # Report summary
        print("Updates summary:")
        # Old version deprecated
        # for tag, newcnt in summary.items():
        #     tag = TagManager.DeserializeTag(tag)
        #     print("Tag {0} has {1} new items, link: {2}".format(
        #         tag, newcnt, GelbooruMan.SearchUrlByTag(tag)))
        ssummary = list(summary.items())
        ssummary.sort(key = lambda x : x[1])
        for tag, newcnt in ssummary:
            if(newcnt==0):
                continue
            tag = TagManager.DeserializeTag(tag)
            print("Tag {0} has {1} new items, link: {2}".format(
                tag, newcnt, GelbooruMan.SearchUrlByTag(tag)))

    def commitFromPid(self, tag, pidUB):
        if not self.tagman().IsExistTag(tag):
            return
        self.updateTagMan1Tag(tag)
        ToCommitIds = []
        newIds = self.tagman().GetAllUncommitedIds(tag)
        for id in newIds:
            if id <= pidUB:
                ToCommitIds.append(id)
        if(len(ToCommitIds)!=0):
            print("Collected {0} unseen pictures, start commit.".format(
                len(ToCommitIds)))
            self.tagman().CommitIds(tag, ToCommitIds)
            print("Commit Finished.")
        else:
            print("No unseen pictures, abandon commit.")

    def commitFromPage(self, tag, page):
        if not self.tagman().IsExistTag(tag):
            return
        pidUB = self.peekTagPagePid(tag,page)
        self.commitFromPid(tag,pidUB)

    def subscribeTag(self, tag: list):
        if self.tagman().IsExistTag(tag):
            print("Tag already exist.")
        else:
            self.tagman().AddTag(tag)
            print("Successsful subscribe tag {0}".format(tag))

    def unsubsribeTag(self, tag: list):
        # Delete all tag_ids has this tag, and remove tag
        if not self.tagman().IsExistTag(conn, tag):
            print("Does not exist this tag.")
        else:
            self.tagman().DeleteTag(tag)

    def listTag(self):
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
        c.execute('''CREATE TABLE IF NOT EXISTS tag_ids_unread (tid INTEGER NOT NULL, pid INTEGER NOT NULL, PRIMARY KEY (tid,pid), FOREIGN KEY(tid) REFERENCES tags(tid));''')
        conn.commit()
        return conn

    # Queries for tag
    def IsExistTag(self, tag):
        tag_s = TagManager.SerializeTag(tag)
        c = self.conn.cursor()
        c.execute('''SELECT tag FROM tags WHERE tag='{0}'; '''.format(tag_s))
        t = c.fetchone()
        return t != None

    def GetAllTags(self):
        c = self.conn.cursor()
        c.execute('SELECT tag FROM tags')
        tags_s = c.fetchall()
        return list(map(lambda tag: TagManager.DeserializeTag(tag[0]), tags_s))

    def AddTag(self, tag: list):
        tag_s = TagManager.SerializeTag(tag)
        c = self.conn.cursor()
        c.execute('''INSERT INTO tags(tag) VALUES ('{0}');'''.format(tag_s))
        self.conn.commit()

    def DeleteTag(self, tag: list):
        tid = self.GetTagId(tag)
        c = self.conn.cursor()
        c.execute(
            '''DELETE FROM tag_ids WHERE tag_ids.tid = {0};'''.format(tid))
        c.execute('''DELETE FROM tags WHERE tags.tid = {0};'''.format(tid))
        self.conn.commit()

    def GetTagId(self, tag: list) -> int:
        tag_s = TagManager.SerializeTag(tag)
        c = self.conn.cursor()
        c.execute(
            '''SELECT tid FROM tags WHERE tags.tag = '{0}';'''.format(tag_s))
        return c.fetchone()[0]

    # Queries for Ids
    def CommitIds(self, tag: list, ids: list):
        if(len(ids)==0):
            return
        tid = self.GetTagId(tag)
        c = self.conn.cursor()
        c.execute('''INSERT INTO tag_ids VALUES ''' +
                  ",".join(list(map(lambda id: "({0},{1})".format(tid, id), ids))) + ';')
        c.execute('''DELETE FROM tag_ids_unread WHERE tag_ids_unread.tid = {0} AND tag_ids_unread.pid IN ({1});'''.format(tid,','.join(list(map(str,ids)))))
        self.conn.commit()

    def AddUncommitedIds(self, tag: list, ids: list):
        if(len(ids)==0):
            return
        tid = self.GetTagId(tag)
        c = self.conn.cursor()
        c.execute('''INSERT INTO tag_ids_unread VALUES ''' +
                  ",".join(list(map(lambda id: "({0},{1})".format(tid, id), ids))) + ';')
        self.conn.commit()

    def GetAllUncommitedIds(self, tag: list) -> list:
        tid = self.GetTagId(tag)
        c = self.conn.cursor()
        c.execute(
            '''SELECT pid FROM tag_ids_unread WHERE tag_ids_unread.tid = {0};'''.format(tid))
        pids = c.fetchall()
        pids = [x[0] for x in pids]
        return pids

    def FilterNewIds(self, tag: list, ids: list) -> list:
        tid = self.GetTagId(tag)
        c = self.conn.cursor()
        c.execute(
            '''SELECT pid FROM tag_ids WHERE tag_ids.tid = {0};'''.format(tid))
        pids = [x[0] for x in c.fetchall()]
        c.execute(
            '''SELECT pid FROM tag_ids_unread WHERE tag_ids_unread.tid = {0};'''.format(tid))
        pids = pids + [x[0] for x in c.fetchall()]
        pset = set(pids)
        newids = []
        for id in ids:
            if id not in pset:
                newids.append(id)
        return newids

    def FilterUncommitedIds(self, tag: list, ids: list) -> int:
        tid = self.GetTagId(tag)
        c = self.conn.cursor()
        c.execute(
            '''SELECT pid FROM tag_ids WHERE tag_ids.tid = {0};'''.format(tid))
        pids = c.fetchall()
        pids = [x[0] for x in pids]
        pset = set(pids)
        newids = []
        for id in ids:
            if id not in pset:
                newids.append(id)
        return newids

    @staticmethod
    def SerializeTag(tag: list) -> str:
        return '+'.join(tag)

    @staticmethod
    def DeserializeTag(tag: str) -> list:
        return tag.split('+')


def Help():
    print("""0-> checkTagUpdates(),
1-> ListTag, 2-> SubscribeTagX, 3-> UnsubscribeTagX,
4-> commitFromPageN,   5-> commitFromPidX,  6-> Help,
9-> Exit""")
if __name__ == '__main__':
    # logging.basicConfig(level=logging.DEBUG)
    gelman = GelbooruMan()
    Dic = {0: gelman.checkUpdates, 1: gelman.listTag, 2: gelman.subscribeTag, 3: gelman.unsubsribeTag,
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
