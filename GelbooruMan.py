import sqlite3
import requests
from bs4 import BeautifulSoup
PageFetchLimit = 500
# Url Ops
def PageUrl(url,pageNum):
	return url + '&pid={0}'.format(pageNum * 42)
def SearchUrlByTag(tags):
	parentUrl = "https://gelbooru.com/index.php?page=post&s=list&tags={0}"
	return parentUrl.format(serializeTag(tags))
# HTML Parsing
def IsLastPage(pageSoup):
	pagination = pageSoup.select('.pagination')[0].contents
	if pagination[-1].name!='a':
		return True
	return False
def ParseIdsFromPage(pageSoup):
	imglist = pageSoup.select('img.preview')
	imgIds = list(map(lambda x : x.parent['id'][1:],imglist))
	# imgHrefs = list(map(lambda x : 'https:' + x.parent['href'],imglist))
	return (imgIds,0)
# DB Ops
def serializeTag(tag : list) -> str:
	return '+'.join(tag)
def deserializeTag(tag : str) -> list:
	return tag.split('+')
def CheckDBSanity():
	conn = sqlite3.connect('tags.db')
	c = conn.cursor()
	c.execute('''CREATE TABLE IF NOT EXISTS tags (tid INTEGER PRIMARY KEY AUTOINCREMENT, tag TEXT not null, UNIQUE (tag));''')
	c.execute('''CREATE TABLE IF NOT EXISTS tag_ids (tid INTEGER NOT NULL, pid INTEGER NOT NULL, PRIMARY KEY (tid,pid), FOREIGN KEY(tid) REFERENCES tags(tid));''')
	conn.commit()
	return conn
def IsExistTag(conn,tag):
	tag_s = serializeTag(tag)
	c = conn.cursor()
	c.execute('''SELECT tag FROM tags WHERE tag='{0}'; '''.format(tag_s))
	t = c.fetchone()
	return len(t)!=0
def GetAllTags(conn):
	c = conn.cursor()
	c.execute('SELECT tag FROM tags')
	tags_s = c.fetchall()
	return list(map(lambda tag: deserializeTag(tag[0]),tags_s))
def AddTag(conn, tag : list):
	tag_s = serializeTag(tag)
	c = conn.cursor()
	c.execute('''INSERT INTO tags(tag) VALUES ('{0}');'''.format(tag_s))
	conn.commit()
def _getTagId(conn, tag : list) -> int:
	tag_s = serializeTag(tag)
	c = conn.cursor()
	c.execute('''SELECT tid FROM tags WHERE tags.tag = '{0}';'''.format(tag_s))
	return c.fetchone()[0]
def commitIds(conn, tag : list,ids : list):
	tid = _getTagId(conn,tag)
	c = conn.cursor()
	c.execute('''INSERT INTO tag_ids VALUES ''' + ",".join(list(map(lambda id : "({0},{1})".format(tid,id),ids))) + ';') 
	conn.commit()
def filterNewIds(conn,tag : list,ids) -> int:
	tid = _getTagId(conn,tag)
	c = conn.cursor()
	c.execute('''SELECT pid FROM tag_ids WHERE tag_ids.tid = {0};'''.format(tid))
	pids = c.fetchall()
	pids = [x[0] for x in pids]
	pset = set(pids)
	newids = []
	for id in ids:
		if int(id) not in pset:
			newids.append(id)
	return newids
# UI funcs
def checkUpdates():
	conn = CheckDBSanity()
	MarkTags = GetAllTags(conn)
	summary = {}
	for MarkTag in MarkTags:
		tag_cnt = 0
		for page in range(0,PageFetchLimit):
			r = requests.get(PageUrl(SearchUrlByTag(MarkTag),page))
			soup = BeautifulSoup(r.text, 'html.parser')
			# Do something to the page
			imgIds,_ = ParseIdsFromPage(soup)
			newcnt = len(filterNewIds(conn,MarkTag,imgIds))
			if newcnt==0 or IsLastPage(soup):
				break
			else:
				tag_cnt += newcnt
		summary[serializeTag(MarkTag)] = tag_cnt
	# Report summary
	print("Updates summary:")
	for tag,newcnt in summary.items():
		tag = deserializeTag(tag)
		print("Tag {0} has {1} new items, link: {2}".format(tag,newcnt,SearchUrlByTag(tag)))
def commitFromPid(tag,pidUB):
	conn = CheckDBSanity()
	if not IsExistTag(conn,tag):
		return
	ToCommitIds = []
	for page in range(0,PageFetchLimit):
			r = requests.get(PageUrl(SearchUrlByTag(tag),page))
			soup = BeautifulSoup(r.text, 'html.parser')
			# Do something to the page
			imgIds,_ = ParseIdsFromPage(soup)
			newIds = filterNewIds(conn,tag,imgIds)
			if len(newIds)==0 or IsLastPage(soup):
				break
			else:
				for id in newIds:
					if int(id) <= pidUB:
						ToCommitIds.append(id)
	print("Collected {0} unseen pictures, start commit".format(len(ToCommitIds)))
	commitIds(conn,tag,ToCommitIds)
	print("Commit Finished")
def commitFromPage(tag,page):
	conn = CheckDBSanity()
	if not IsExistTag(conn,tag):
		return
	ToCommitIds = []
	for page in range(page,PageFetchLimit):
			r = requests.get(PageUrl(SearchUrlByTag(tag),page))
			soup = BeautifulSoup(r.text, 'html.parser')
			# Do something to the page
			imgIds,_ = ParseIdsFromPage(soup)
			newIds = filterNewIds(conn,tag,imgIds)
			if len(newIds)==0 or IsLastPage(soup):
				break
			else:
				for id in newIds:
					if int(id) <= pidUB:
						ToCommitIds.append(id)
	print("Collected {0} unseen pictures, start commit".format(len(ToCommitIds)))
	commitIds(conn,tag,ToCommitIds)
	print("Commit Finished")
def SubscribeTag(tag : list):
	conn = CheckDBSanity()
	if IsExistTag(conn,tag):
		print("Tag already exist.")
	else:
		AddTag(conn,tag)
		print("Successsful subscribe tag {0}".format(tag))
def UnsubsribeTag(tag: list):
	# Delete all tag_ids has this tag, and remove tag
	pass
def ListTag():
	conn = CheckDBSanity()
	MarkTags = GetAllTags(conn)
	print("Currently subscribe to {0} tags".format(len(MarkTags)))
	print(MarkTags)
def Help():
	print("""0-> checkTagUpdates(),
1-> ListTag, 2-> SubscribeTagX, 3-> UnsubscribeTagX(not),
4-> commitFromPageN,   5-> commitFromPidX,  6-> Help,
9-> Exit""")
if __name__ == '__main__':
	Dic = {0:checkUpdates, 1:ListTag,2:SubscribeTag,3:UnsubsribeTag
		,4:commitFromPage, 5:commitFromPid,6:Help, 9:exit}
	Help()
	while True:
		Sel = int(input(">>"))
		if(4<=Sel<=5):
			Tag = deserializeTag(input("Tag>>"))
			Num = int(input("Pid>>"))
			Dic[Sel](Tag,Num)
		elif (2<=Sel<=3):
			Tag = deserializeTag(input("Tag>>"))
			Dic[Sel](Tag)
		else:
			Dic[Sel]()