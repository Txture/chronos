package org.chronos.chronodb.internal.api;

import org.chronos.chronodb.api.Branch;
import org.jetbrains.annotations.NotNull;

public interface BranchEventListener {

    public void onBranchCreated(@NotNull Branch branch);

    public void onBranchDeleted(@NotNull Branch branch);

}
