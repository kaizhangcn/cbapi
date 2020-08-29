from setuptools import setup

setup(
    name='cbapi',
    url='https://github.com/kaizhangcn/cbapi',
    author='Kai Zhang',
    author_email='kai.zhang.baruchmfe@gmail.com',
    packages=['cbapi'],
    install_requires=['pandas', 'requests'],
    version='1.0.0',
    description='Downloading and presenting organization and people data from Crunchbase.',
    long_description=open('README.md').read(),
)