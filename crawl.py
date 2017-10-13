import urllib2
import re
import urlparse
import robotparser
import datetime
import time
import sys
import random

#sys.path.append('D:\Python27\spider')
import html_parser
from html_parser import ScrapeCallback

class downloader:
	def __init__(self, cache=None, delay=1,
				user_agent='Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11', 
				proxies = None, num_retries=2):
		self.cache = cache
		self.user_agent = user_agent
		self.proxies = proxies
		self.num_retries = num_retries
		self.throttle = Throttle(delay)
	
	def __call__(self, url):
		result = None
		if self.cache:
			try:
				result = self.cache[url]
				except KeyError:
					# url is not available in cache
					pass
				else:
					if self.num_retries > 0 and \
						500 <= result['code'] < 600:
						#server error so ignore result from cache
						#and re-download
						result = None
		if result is None:
			#result was not loaded from cache
			#so still need to download
			self.throttle.wait(url)
			proxy = random.choice(self.proxies) if self.proxies
				else None
			headers = {'User-agent': self.user_agent}
			result = self.download(url, headers, proxy, self.num_retries)
			if self.cache:
				# save result tp cache
				self.cache[url] = result
		return result['html']
		
	#from parser import ScrapeCallback
	#modified to support proxy
	def download(url, headers, proxy, num_retries):
		print 'Downloading:', url
		request = urllib2.Request(url, headers)
		#request = urllib2.Request(url)		
		opener = urllib2.build_opener()
		if proxy:
			proxy_params = {urlparse.urlparse(url).scheme:proxy}
			opener.add_handler(urllib2.ProxyHandler(proxy_params))
		try:
			html = opener.open(request).read()
		except urllib2.URLError as e:
			print e.code
			print 'Download error:', e.reason
			html = None
			if num_retries > 0:
				if hasattr(e, 'code') and 500 <= e.code < 600:
					#recursively  retry 5xx HTTP errors
					return download(url, num_retries-1)
		return html
	
def download_test():
	response = urllib2.urlopen('http://example.webscraping.com')
	html = response.read()
	print html
#def crawl_sitemap(url):
#	sitemap = download(url)
#	links = re.findall()

def get_links(html):
	#retur a list of links from html
	#a regular expression to extract all links from the webpage
	webpage_regex = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
	#return a list include all links from the webpage
	return webpage_regex.findall(html)
def link_crawler(seed_url, delay=1, link_regex=None, proxy=None, max_depth=2, user_agent='wswp', num_retries=2, scrape_callback= ScrapeCallback()):
	crawl_queue = [seed_url]
	seen = {seed_url:0}
	rp = robotparser.RobotFileParser()
	
	while crawl_queue:
		url = crawl_queue.pop()
		#detect whether this url is baned
		#if rp.can_fetch(user_agent, url):
		throttle.wait(url)
		depth = seen[url]
		if depth != max_depth:
			html = download(url, user_agent=user_agent,num_retries=num_retries)
			#html parser
			if scrape_callback:
				scrape_callback(url, html)
			for link in get_links(html):
				if re.search(link_regex, link):
					link = urlparse.urljoin(seed_url, link)
					if link not in seen:
						seen[link] = depth + 1
						crawl_queue.append(link)
		#else:
		#	print 'Blocked by robot.txt:', url
			
#config delay bttween two downloads			
class Throttle:
	def __init__(self, delay):
		self.delay = delay
		self.domains = {}
		
	def wait(self, url):
		domain = urlparse.urlparse(url).netloc
		last_accessed = self.domains.get(domain)
		
		if self.delay > 0 and last_accessed is not None:
			sleep_secs = self.delay - (datetime.datetime.now() - last_accessed).seconds
			if sleep_secs > 0:
				#domain has benn accessed recently
				#so need to sleep
				time.sleep(sleep_secs)
		self.domains[domain] = datetime.datetime.now()

if __name__ == '__main__':
	html_parser.test()
	link_crawler('http://example.webscraping.com', link_regex='/(view|index)')
	#input("Prease <enter>")
	#download('http://example.webscraping.com')
