import datetime
import projex.text

from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class ArchiveMixin(object):
    def onPostCommit(self, event):
        # save archive information after a commit
        if not self.isRecord():
            raise orb.errors.RecordNotFound(type(self), self.id())

        model = self.schema().archiveModel()
        if not model:
            raise orb.errors.ArchiveNotFound(self.schema().name())

        try:
            last_archive = self.archives().last()
        except AttributeError:
            raise orb.errors.ArchiveNotFound(self.schema().name())

        number = last_archive.archiveNumber() if last_archive else 0

        # create the new archive information
        values = self.collectData(inflated=False, flags=~orb.Column.Flags.Primary)
        locale = values.get('locale') or event.context.locale
        record = model(**values)
        record.setArchivedAt(datetime.datetime.now())
        record.setArchiveNumber(number + 1)
        record.setRecordValue(projex.text.camelHump(self.schema().name()), self)

        try:
            record.setLocale(locale)  # property on archive for translatable models
        except AttributeError:
            pass

        record.commit()
