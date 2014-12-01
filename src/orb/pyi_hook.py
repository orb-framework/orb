""" Defines the hook required for the PyInstaller to use projexui with it. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

import os
import projex.pyi

# bake the mako statement files into python files
basepath = os.path.dirname(__file__)
for root, folders, files in os.walk(os.path.join(basepath)):
    for file in files:
        if file.endswith('.mako'):
            src = os.path.join(root, file)
            targ = src.replace('.', '_') + '.py'
            with open(src, 'r') as f:
                data = f.read()
            
            with open(targ, 'w') as f:
                f.write(u'TEMPLATE = r"""\n{0}\n"""'.format(data))

# load the importable objects
hiddenimports, _ = projex.pyi.collect(basepath)