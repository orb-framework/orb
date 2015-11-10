""" Defines the hook required for the PyInstaller to use projexui with it. """

import os
import projex.pyi

# bake the mako statement files into python files
basepath = os.path.dirname(__file__)
for root, folders, files in os.walk(os.path.join(basepath)):
    for file_ in files:
        if file_.endswith('.mako'):
            src = os.path.join(root, file_)
            targ = src.replace('.', '_') + '.py'
            with open(src, 'r') as f:
                data = f.read()
            
            with open(targ, 'w') as f:
                f.write(u'TEMPLATE = r"""\n{0}\n"""'.format(data))

# load the importable objects
hiddenimports, _ = projex.pyi.collect(basepath)