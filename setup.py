from distutils.core import setup

from setuptools import find_packages

setup(
  name='bitcoin_deposit_worker',
  packages=find_packages(), # this must be the same as the name above
  version='0.4',
  description='A random test lib',
  author='Yuanfei Zuu',
  author_email='hi@yuanfeiz.cn',
  url='https://github.com/yuanfeiz/bitcoin_deposit_worker', # use the URL to the github repo
  download_url='https://github.com/yuanfeiz/bitcoin_deposit_worker/archive/master.zip', # I'll explain this in a second
  keywords=['testing', 'logging', 'example'], # arbitrary keywords
  classifiers=[],
)
