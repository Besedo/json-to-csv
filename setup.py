from setuptools import setup
from setuptools import find_packages


setup(name='json-to-csv',
      version='0.0.1',
      description='Transform a json file to a csv file',
      author='YaYaB',
      author_email='bezzayassine@gmail.com',
      url='https://github.com/Besedo/json-to-csv',
      download_url='https://github.com/Besedo/json-to-csv',
      license='MIT',
      classifiers=['License :: MIT License',
                   'Programming Language :: Python',
                   'Operating System :: Microsoft :: Windows',
                   'Operating System :: POSIX',
                   'Operating System :: Unix',
                   'Operating System :: MacOS',
                   'Programming Language :: Python :: 3',
                   'Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 3.5',
                   ],
      install_requires=[],
      extras_require={},
      packages=find_packages(),
      entry_points={
          'console_scripts': [
              'json-to-csv=json_to_csv.json_to_csv:main_cli',
          ]},

      )
