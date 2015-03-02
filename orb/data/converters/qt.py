""" Defines a mapper for converting Qt information to and from the database. """

import binascii
import logging

from projex.text import nativestring as nstr
from projex.lazymodule import lazy_import
from ..converter import DataConverter

log = logging.getLogger(__name__)
QtCore = lazy_import('xqt.QtCore')


class QDataConverter(DataConverter):
    """
    Maps Qt values to standard python values.
    """
    def convert(self, value):
        """
        Converts the inputted value to a Python value.
        
        :param      value | <variant>
        
        :return     <variant>
        """
        val_name = type(value).__name__
        
        if val_name == 'QString':
            return nstr(value.toUtf8())
        elif val_name == 'QVariant':
            return value.toPyObject()
        elif val_name == 'QDate':
            return value.toPyDate()
        elif val_name == 'QDateTime':
            return value.toPyDateTime()
        elif val_name == 'QTime':
            return value.toPyTime()
        elif val_name == 'QColor':
            return value.name()
        
        elif val_name == 'QIcon':
            try:
                pixmap = value.pixmap(value.actualSize(QtCore.QSize(256, 256)))
                
                arr = QtCore.QByteArray()
                buf = QtCore.QBuffer()
                buf.setBuffer(arr)
                buf.open(QtCore.QBuffer.WriteOnly)
                pixmap.save(buf, 'PNG')
                buf.close()
                
                return binascii.b2a_base64(nstr(arr))
            except ImportError:
                log.error('Cannot store QIcon, xqt.QtCore not defined.')
        
        elif val_name == 'QPixmap':
            try:
                arr = QtCore.QByteArray()
                buf = QtCore.QBuffer()
                buf.setBuffer(arr)
                buf.open(QtCore.QBuffer.WriteOnly)
                value.save(buf, 'PNG')
                buf.close()
                
                return binascii.b2a_base64(nstr(arr))
            except ImportError:
                log.error('Cannot store QPixmap, xqt.QtCore not defined.')
        
        return super(QDataConverter, self).convert(value)

# register the Qt data converter
DataConverter.registerAddon('Qt', QDataConverter())