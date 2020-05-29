package org.chronos.chronosphere.test.testmodels.instance;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import java.util.Set;

public interface TestModel extends Iterable<EObject> {

    public EPackage getEPackage();

    public Set<EObject> getAllEObjects();

    public EObject getEObjectByID(String id);

}
