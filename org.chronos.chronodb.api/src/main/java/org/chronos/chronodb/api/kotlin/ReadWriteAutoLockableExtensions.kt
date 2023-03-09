package org.chronos.chronodb.api.kotlin

import org.chronos.common.autolock.ReadWriteAutoLockable

object ReadWriteAutoLockableExtensions {

    inline fun <T> ReadWriteAutoLockable.withNonExclusiveLock(action: ()->T): T {
        return this.lockNonExclusive().use {
            action()
        }
    }

    inline fun <T> ReadWriteAutoLockable.withExclusiveLock(action: ()->T): T {
        return this.lockExclusive().use {
            action()
        }
    }


}