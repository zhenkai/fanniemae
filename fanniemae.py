from argparse import ArgumentParser
from tempfile import NamedTemporaryFile
from os import path, unlink
from time import time, strftime, sleep
import shutil
import urllib2
import json
import logging
import zlib

LOGIN_URL = 'https://loanperformancedata.fanniemae.com/lppub/loginForm.html'
#DOWNLOAD_URL = 'https://loanperformancedata.fanniemae.com/lppub-docs/publish/%s_%sQ%s.txt.gz'
DOWNLOADS_URL= 'https://loanperformancedata.fanniemae.com/lppub/getMonthlyDownloadJson.json?_=%s&_search=false&nd=%s&rows=20&page=1&sidx=&sord=asc'
# input your proxy url, something like 'http://username:password@webproxy.bankofamerica.com:8080'
PROXY_URL=None
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
    self.download_list = []
    if proxy_url is None:
      self.opener = urllib2.build_opener()
    else:
      self.opener = urllib2.build_opener(
                      urllib2.HTTPHandler(),
                      urllib2.HTTPSHandler(),
                      urllib2.ProxyHandler({'https': proxy_url}))

  def login(self):
    logging.info('Login...')
    logging.info('Login succeeded without actually doing anything')

    # due to the stupidity in fanie mae's code
    # (their javascript shows that they're not really checking username and password at all)
    # login is not necessary to download the code
    # let's do this short cut as long as they keep their stupid code
    # be quiet about this fact

  def __exit__(self, type, value, traceback):
    pass


  def download(self, url, show_progress):
    gz_filename = url.split('/')[-1]
    filename = '.'.join(gz_filename.split('.')[:-1])
    local_filename = path.join(self.dir, filename)
    if path.exists(local_filename):
      logging.info('%s already exists. Skip downloading.', local_filename)
      return

    try:
      r = None
      r = self.opener.open(getRequestWithHeaders(url))
      content_length = int(r.headers.get('content-length'))
      logging.info('Downloading %s (%0.2f MB) to %s' % (filename, content_length / (1024.0 * 1024.0), self.dir))

      if show_progress:
        bar_to_use = ProgressBar
      else:
        bar_to_use = NoopProgressBar

      with bar_to_use(content_length, 50, '#') as bar:
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        f = NamedTemporaryFile(delete=False)
        chunk = r.read(4096)
        while chunk:
          decompressed_chunk = decompressor.decompress(chunk)
          f.write(decompressed_chunk)
          bar.update(len(chunk))
          chunk = r.read(4096)

        f.close()
        shutil.move(f.name, local_filename)

    except Exception as e:
      logging.error('Downloading %s failed with status: %s' % (filename, e))
      raise e
    else:
      logging.info('%s Downloaded' % filename)
    finally:
      if path.exists(f.name):
        f.close()
        unlink(f.name)
      if r is not None:
        r.close()

  def list_downloads(self, url):
    try:
      r = self.opener.open(getRequestWithHeaders(url))
      json_response = json.loads(r.read())
    except urllib2.URLError as e:
      logging.error('Cannot list downloads: %s', e)
      return []
    download_list = []

    def process_archive(archive):
        filename = archive[3]
        quarter = int(filename.split('.')[0][-1])
        download_list.append((archive[0], int(archive[2]), quarter, archive[4] + '/publish/' + filename))

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

  def download_all(self, url, from_year, to_year, show_progress = False, acq_only = False, perf_only = False, quarters = [1,2,3,4]):
    def is_needed(filetype, year, quarter):
      if year < from_year or year > to_year:
        return False
      if quarter not in quarters:
        return False
      if acq_only and filetype == 'Performance':
        return False
      if perf_only and filetype == 'Acquisitions':
        return False

      return True

    download_list = filter(lambda (filetype, year, quarter, link): is_needed(filetype, year, quarter), self.list_downloads(url))
    self.download_list = download_list
    map(lambda (filetype, year, quarter, link): self.download(link, show_progress), download_list)

  def download_all_in_list(self, download_list, show_progress=False):
    map(lambda (filetype, year, quarter, link): self.download(link, show_progress), download_list)

if __name__ == '__main__':
  parser = ArgumentParser(description='FannieMae load data download hack')
  parser.add_argument('dir', help='download directory')
  parser.add_argument('-f', '--from-year', help='from year', type=int, default=START_YEAR)
  parser.add_argument('-t', '--to-year', help='to year', type=int, default=END_YEAR)
  parser.add_argument('-p', '--progress', help='show progress bar', action='store_true', default=False)
  parser.add_argument('--acq-only', help='only download acquisition file', action='store_true', default=False)
  parser.add_argument('--perf-only', help='only download performance file', action='store_true', default=False)
  parser.add_argument('-q', '--quarters', help='quarters to download', nargs='+', type=int, default=[1,2,3,4])
  parser.add_argument('-r', '--retry', help='number of times to retry', type=int, default=0)
  args = parser.parse_args()

  logging.basicConfig(format='>>> [%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)

  if not path.isdir(args.dir):
    logging.error('%s is not a directory!' % args.dir)
    exit(1)

  if args.acq_only and args.perf_only:
    logging.error('You set both --acq-only and --perf-only. Which one do you really want? Acquisition or Performance?')
    exit(1)

  if args.retry < 0:
    logging.error('Retry must be a non-negative number')
    exit(1)

  interval = 5
  finished = False
  fm = FannieMaeLoanData(args.dir)
  fm.login()
  for i in xrange(args.retry + 1):
    try:
      millis = int(round(time() * 1000))
      if i == 0:
        fm.download_all(DOWNLOADS_URL % (millis, millis), args.from_year, args.to_year, args.progress, args.acq_only, args.perf_only, args.quarters)
      else:
        fm.download_all_in_list(fm.download_list, args.progress)
      logging.info('All downloads finished. Bye bye bye...')
      finished = True
      break
    except Exception as e:
      logging.error('Downloads aborted! Error: %s', e)
      logging.info('Retry attempt %s in %s seconds', i + 1, interval)
      sleep(interval)
      interval = interval * 2
    except:
      logging.error('Downloads interruptted!')
      logging.info('Retry attempt %s in %s seconds', i + 1, interval)
      sleep(interval)
      interval = interval * 2

  if not finished:
    logging.error('Cannot finish downloads after %s retries :(')
