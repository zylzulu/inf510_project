import requests
import json
from bs4 import BeautifulSoup
import sqlite3
from urllib import parse
import socket
import pycountry
import argparse 

# conn = sqlite3.connect('final_project.db')
# cur = conn.cursor()


def crawler(url, world_stat):
	try:
		r = requests.get(url)
		r.raise_for_status()
	except requests.exceptions.RequestException as e:
		print(e)	
	soup = BeautifulSoup(r.content,'lxml')
	main_table = soup.findAll('table', {"bgcolor":"#666699"})[-1]
	for row in main_table.findAll('tr')[2:-2]:
		country_stat = dict()
		info = row.findAll('td')
		country = info[0].b.text.replace('\n','').replace('\r','')
		if not info[-4].b:
			Internet_user = info[-4].font.text.replace('\n','').replace('\r','').replace(',','')
		else:
			Internet_user = info[-4].b.text.replace('\n','').replace('\r','').replace(',','')
		if info[-4].font.text == 'n/a':
			Internet_user = 0
		penetration = info[-3].font.text.replace('\n','').replace('\r','').replace('%','')
		if penetration == 'n/a':
			penetration = 0
		facebook_user = info[-1].font.text.replace('\n','').replace('\r','').replace(',','')
		if facebook_user == 'n/a':
			facebook_user = 0
		country_stat['country'] = country
		country_stat['Internet_user'] = Internet_user
		country_stat['penetration'] = penetration
		country_stat['facebook_user'] = facebook_user
		world_stat.append(country_stat)


def get_infringing_urls(notice_id, notice_info, host_list):
	headers = {'accept':'application/json/', 'content-type':'application/json'}
	visited_url = []
	for work in notice_info['works']:
		for urls in work['infringing_urls']:
			if not urls:
				continue
			host_info = dict()
			url = urls['url']
			host = parse.urlsplit(url)[1]			
			if host in visited_url:
				continue
			else:
				visited_url.append(host)	
				try:
					ip = socket.gethostbyname(host)
				except Exception as e:
					print('This url is no longer accessible')
					continue
				ip_info = requests.request('GET','https://freegeoip.app/json/' + ip, headers = headers).json()
				if ip_info['country_name']:
					host_info['url'], host_info['country'], host_info['notice_id']= host, ip_info['country_name'], notice_id
					host_list.append(host_info)


def api_scraper(url, notice, host_info):
	header={"User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0", 
	    "X-Authentication-Token": "kVxB3P281z_dhMzxjsBs"}
	try:
		r = requests.get(url, headers = header)
		r.raise_for_status()
		res = r.json()
	except requests.exceptions.RequestException as e:
		print(e)
	if res:
		noti = dict()
		for key in res.keys():
			notice_type = key
		notice_info = res[notice_type]
		notice_id = notice_info['id']
		sender = notice_info['sender_name'].strip()
		recipient = notice_info['recipient_name'].strip()
		jurisdictions = notice_info['jurisdictions']
		get_infringing_urls(notice_id, notice_info, host_info)
		noti['id'] = notice_id
		if notice_type and sender and recipient and jurisdictions:
			noti['type'] = notice_type
			noti['sender'] = sender
			noti['recipient'] = recipient
			noti['jurisdictions'] = jurisdictions
			notice.append(noti)
	else:
		print("no information about this notice")


def remote_mode(cur, conn, s):
	cur.execute('drop table if exists notice')
	cur.execute('drop table if exists stat')
	cur.execute('drop table if exists host')
	cur.execute('drop table if exists relation_between_notice_stat')
	sql1 = 'CREATE TABLE IF NOT EXISTS stat (id INTEGER PRIMARY KEY AUTOINCREMENT, country TEXT, Internet_user INTEGER, penetration REAL, facebook_user INTEGER);'
	cur.execute(sql1)
	cur.execute('create table if not exists notice(notice_id INTEGER PRIMARY KEY, type text, sender text, recipient text)')
	cur.execute('create table if not exists relation_between_notice_stat(id INTEGER PRIMARY KEY AUTOINCREMENT, notice_id INTEGER, stat_id INTEGER)')
	cur.execute('create table if not exists host(id INTEGER PRIMARY KEY AUTOINCREMENT, url text, country text, notice_id INTEGER, country_id INTEGER)')
	region = ['1', '2', '3', '4', '5', '6']
	world_stat = list()
	for rg in region:
		url1 = f'https://www.internetworldstats.com/stats{rg}.htm'
		try:
			crawler(url1, world_stat)
		except Exception as e:
			print(e)
			continue
	for w in world_stat:
		try:
			cur.execute('insert into stat values(NULL, ?,?,?,?)',(w['country'],int(w['Internet_user']),float(w['penetration']),int(w['facebook_user'])))
			conn.commit()
		except:
			continue
	
	# get data about take down notice from Lumen

	notice = list()
	host_list = list()     
	notice_id_list = range(19600000,19644000, s)
	for notice_id in notice_id_list:
		url2 = f'https://lumendatabase.org/notices/{notice_id}.json'
		try:
			print('scraping from API...', notice_id)
			api_scraper(url2, notice, host_list)
		except:
			continue
	for h in host_list:
		if h['url'] and h['country'] and h['notice_id']:
			try:
				country_id = cur.execute('select id from stat where country = ?',(h['country'],)).fetchone()[0]
				if country_id:
					cur.execute('insert into host values(NULL, ?,?,?,?)',(h['url'], h['country'], h['notice_id'], country_id))
					conn.commit()
				else:
					continue
			except Exception as e:
				print('No this country recorded in world Internet Statistics')
				continue
		else:
			continue

	for n in notice:
		if n['id'] and n['type'] and n['sender'] and n['recipient'] and n['jurisdictions']:
			try:
				cur.execute('insert into notice values (?,?,?,?)',(n['id'], n['type'], n['sender'], n['recipient']))
				conn.commit()
			except Exception as e:
				print(e)
				continue
			for j in n['jurisdictions']:
				try:
					country = pycountry.countries.get(alpha_2=j.upper()).name
					country_id = cur.execute('select id from stat where country = ?',(country,)).fetchone()[0]
					if country_id:
						cur.execute('insert into relation_between_notice_stat values (NULL, ?, ?)',(n['id'], country_id))
						conn.commit()
					else:
						continue
				except Exception as e:
					print('No this country recorded in world Internet Statistics')
					continue
		else:
			continue	


def local_mode(cur, conn):
	table_name = ['notice','host','relation_between_notice_stat','stat']
	for t in table_name:
		cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{t}'")
		fetch_one = cur.fetchone()
		if not fetch_one:
			print(f'Table \'{t}\' does not exist in database, and now jump to the \'test\' method.')
			remote_mode(cur, conn, 100)
			return
	notice = cur.execute('select * from notice').fetchall() #data about take down notice
	host = cur.execute('select * from host').fetchall() # data about geolocation of the website host
	relation_between_notice_stat = cur.execute('select * from relation_between_notice_stat').fetchall() #using the 'jurisdiction' field to connect two tables
	world_stat = cur.execute('select * from stat').fetchall() #statistical data of Internet penetration all over the world
	conn.commit()
	for n in notice:
		print(n)
	for h in host:
		print(h)
	for w in world_stat:
		print(w)
	for r in relation_between_notice_stat:
		print(r)



def main():
	conn = sqlite3.connect('final_project.db')
	cur = conn.cursor()
#	cur.execute('drop table notice')
#	cur.execute('drop table stat')
#	cur.execute('drop table relation_between_notice_stat')

	parser = argparse.ArgumentParser()
	parser.add_argument('-source', choices=['local', 'remote', 'test'], nargs=1, help="where data should be gotten from")
	args = parser.parse_args()
	location = args.source[0]
	
	if location == 'local': 
		local_mode(cur, conn)
	elif location == 'remote':
		remote_mode(cur, conn, 100)
	else:
		remote_mode(cur, conn, 1000)


#print(cur.execute('select * from notice').fetchall())
#print(cur.execute('select * from host').fetchall())
#print(cur.execute('select * from relation_between_notice_stat').fetchall())
if __name__ == '__main__':
	main()