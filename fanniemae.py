from gzip import open as gopen
from argparse import ArgumentParser
from os import path
from time import time, strftime
import urllib2
import json
import logging

LOGIN_URL = 'https://loanperformancedata.fanniemae.com/lppub/loginForm.html'
#DOWNLOAD_URL = 'https://loanperformancedata.fanniemae.com/lppub-docs/publish/%s_%sQ%s.txt.gz'
DOWNLOADS_URL= 'https://loanperformancedata.fanniemae.com/lppub/getMonthlyDownloadJson.json?_=%s&_search=false&nd=%s&rows=20&page=1&sidx=&sord=asc'
# input your proxy url, something like 'http://username:password@webproxy.bankofamerica.com:8080'
PROXY_URL='http://122.227.164.124:8118'
START_YEAR = 1900
END_YEAR = 2100

class ProgressBar(object):
  ''' A hack to show download progress '''
  def __init__(self, content_length, bar_length, char):
    self.content_length = content_length
    self.bar_length = bar_length
    self.char = char
    self.bytes_read = 0
    self.chars_written = 0
    from sys import stdout
    self.stdout = stdout

  def __enter__(self):
    # setup progress bar
    # this is hacky
    self.stdout.write('>>> [%s] [INFO] [%s]' % (strftime("%Y-%m-%d %H:%M:%S,000"), (' ' * self.bar_length)))
    self.stdout.flush()
    # return to start of line, after '['
    self.stdout.write('\b' * (self.bar_length + 1))
    return self

  def __exit__(self, type, value, traceback):
    self.stdout.write('\n')
    self.stdout.flush()

  def update(self, new_bytes_read):
    self.bytes_read = self.bytes_read + new_bytes_read
    expected_chars = int(self.bar_length *self.bytes_read / self.content_length)
    if (self.chars_written < expected_chars):
      self.stdout.write(self.char * (expected_chars - self.chars_written))
      self.stdout.flush()
      self.chars_written = expected_chars

class NoopProgressBar(object):
  ''' A noop progress bar '''
  def __init__(self, content_length, bar_length, char):
    pass

  def update(self, new_bytes_read):
    pass

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    pass


# an attempt to solve the 104 error in boa's network
def getRequestWithHeaders(url):
  req = urllib2.Request(url)
  req.add_header('User-Agent', 'Mozilla/5.0')
  req.add_header('Connection', 'keep-alive')
  return req

class FannieMaeLoanData(object):
  ''' A hack to download Fannie Mae loan data'''

  def __init__(self, directory, login_url=LOGIN_URL, proxy_url=PROXY_URL):
    self.login_url = login_url
    self.dir = directory
    self.opener = urllib2.build_opener(
                    urllib2.HTTPHandler(),
                    urllib2.HTTPSHandler(),
                    urllib2.ProxyHandler({'https': proxy_url}))

  def __enter__(self):
    logging.info('Login...')
    logging.info('Login succeeded without actually doing anything')

    # due to the stupidity in fanie mae's code
    # (their javascript shows that they're not really checking username and password at all)
    # login is not necessary to download the code
    # let's do this short cut as long as they keep their stupid code
    # be quiet about this fact
    return self

  def __exit__(self, type, value, traceback):
    logging.info('All downloads finished. Bye bye bye...')


  def download(self, url, show_progress):
    filename = url.split('/')[-1]
    try:
      r = self.opener.open(getRequestWithHeaders(url))
      content_length = int(r.headers.get('content-length'))
      logging.info('Downloading %s (%0.2f MB) to %s' % (filename, content_length / (1024.0 * 1024.0), self.dir))

      if show_progress:
        bar_to_use = ProgressBar
      else:
        bar_to_use = NoopProgressBar

      with bar_to_use(content_length, 50, '#') as bar:
        local_filename = path.join(self.dir, filename)
        with open(local_filename, 'wb') as f:
          chunk = r.read(4096)
          while chunk:
            f.write(chunk)
            bar.update(len(chunk))
            chunk = r.read(4096)
    except urllib2.URLError as e:
      logging.error('Downloading %s failed with status: %s' % (filename, e.code))
    else:
      logging.info('%s Downloaded' % filename)
      self.decompress(local_filename)
    finally:
      r.close()

  def decompress(self, filename):
    basename = filename.split('.')[:-1]
    txt_file = '.'.join(basename)
    logging.info('Decompressing %s to %s', filename, txt_file)
    with open(txt_file, 'w') as tf:
      with gopen(filename, 'rb') as gf:
        buffer = gf.read(4096)
        while buffer:
          tf.write(buffer)
          buffer = gf.read(4096)
    logging.info('Decompressing %s finished', filename)

  def list_downloads(self, url):
    try:
      r = self.opener.open(getRequestWithHeaders(url))
      json_response = json.loads(r.read())
    except urllib2.URLError as e:
      logging.error('Cannot list downloads: %s', e.code)
      return []
    download_list = []

    def process_archive(archive):
        download_list.append((int(archive[2]), archive[4] + '/publish/' + archive[3]))

    def process_archives_of_quarter(archives_of_quarter):
      # filter out unavailable quaters first before processing
      available_archives = filter(lambda archive: len(archive) > 3, archives_of_quarter)
      map(process_archive, available_archives)

    def process_archives_of_year(archives_of_year):
      archives_of_quarters = map(lambda quarter: archives_of_year.get('archiveFilesQ%s' % quarter), range(5)[1:])
      map(process_archives_of_quarter, archives_of_quarters)

    try:
      map(process_archives_of_year, json_response.get(u'downloadDocInfoList'))
    except (AttributeError, TypeError, IndexError, ValueError) as ex:
      logging.error('Cannot list downloads. They must have changed the json format. %s', repr(ex))
      return []

    return download_list

  def download_all(self, url, from_year, to_year, show_progress = False):
    download_list = filter(lambda (year, link): year >= from_year and year <= to_year, self.list_downloads(url))
    map(lambda (year, link): self.download(link, show_progress), download_list)

if __name__ == '__main__':
  parser = ArgumentParser(description='FannieMae load data download hack')
  parser.add_argument('dir', help='download directory')
  parser.add_argument('-f', '--from-year', help='from year', type=int, default=START_YEAR)
  parser.add_argument('-t', '--to-year', help='to year', type=int, default=END_YEAR)
  parser.add_argument('-p', '--progress', help='show progress bar', action='store_true', default=False)
  args = parser.parse_args()

  logging.basicConfig(format='>>> [%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)

  if not path.isdir(args.dir):
    logging.error('%s is not a directory!' % args.dir)
    exit(1)

  with FannieMaeLoanData(args.dir) as fm:
    millis = int(round(time() * 1000))
    fm.download_all(DOWNLOADS_URL % (millis, millis), args.from_year, args.to_year, args.progress)
