from distutils.core import setup

setup(
      name = 'KommatiPara',
      version = '1.0.0',
      py_modules = ['main'],
      author ='GiuseppeFRN',
      author_email = 'giuseppe.fur96@gmail.com',
      url = 'https://github.com/giuseppefrn/KommatiPara',
      description = 'A simple ETL process on financial dataset',

      python_requires='==3.7.*',
      install_requires=[
            'pyspark==3.4.0'
    ]
)