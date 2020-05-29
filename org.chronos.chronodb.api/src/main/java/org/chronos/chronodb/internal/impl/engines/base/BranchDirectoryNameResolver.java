package org.chronos.chronodb.internal.impl.engines.base;

@FunctionalInterface
public interface BranchDirectoryNameResolver {

    public String createDirectoryNameForBranchName(String branchName);

    public static final BranchDirectoryNameResolver NULL_NAME_RESOLVER = branchName -> null;
}
