package org.chronos.chronodb.test.base;

import com.google.common.collect.Maps;
import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.builder.database.spi.ChronoDBBackendProvider;
import org.chronos.chronodb.api.builder.database.spi.TestSuitePlugin;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.impl.base.builder.database.service.ChronoDBBackendProviderService;
import org.chronos.common.test.ChronosUnitTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assume.*;

@RunWith(Parameterized.class)
public abstract class AllBackendsTest extends ChronosUnitTest {

	// =====================================================================================================================
	// JUNIT PARAMETERIZED TEST DATA
	// =====================================================================================================================

	@Parameters(name = "Using {0}")
	public static Collection<Object[]> data() {
		return ChronoDBBackendProviderService.getInstance().getAvailableBuilders().stream()
			.map(ChronoDBBackendProvider::getBackendName)
			.map(name -> new Object[]{name})
			.collect(Collectors.toSet());
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	@Parameter
	public String backend;

	private TestSuitePlugin backendTestSuitePlugin;

	// =====================================================================================================================
	// API
	// =====================================================================================================================

	protected String getChronoBackendName() {
		return this.backend;
	}

	protected TestSuitePlugin getBackendTestSuitePlugin(String backend){
		checkNotNull(backend, "Precondition violation - argument 'backend' must not be NULL!");
		if(backend.equals(this.backend)){
			this.getBackendTestSuitePlugin();
		}
		return this.loadTestSuitePlugin(backend);
	}

	protected TestSuitePlugin getBackendTestSuitePlugin(){
		if(this.backend == null){
			throw new IllegalStateException("There is no current backend! Did the test start?");
		}
		if(this.backendTestSuitePlugin == null){
			this.backendTestSuitePlugin = this.loadTestSuitePlugin(this.backend);
		}
		return this.backendTestSuitePlugin;
	}

	private TestSuitePlugin loadTestSuitePlugin(String backend){
		ChronoDBBackendProvider provider = ChronoDBBackendProviderService.getInstance().getBackendProvider(backend);
		TestSuitePlugin testSuitePlugin = provider.getTestSuitePlugin();
		assumeThat("Backend Provider [" + backend + "] does not support the test suite plug-in!", testSuitePlugin, is(notNullValue()));
		return testSuitePlugin;
	}

	protected Configuration createChronosConfiguration(String backend){
		checkNotNull(backend, "Precondition violation - argument 'backend' must not be NULL!");
		TestSuitePlugin testSuitePlugin = this.getBackendTestSuitePlugin(backend);
		Configuration configuration = testSuitePlugin.createBasicTestConfiguration(this.getCurrentTestMethod(), this.getTestDirectory());
		if(configuration == null){
			throw new IllegalStateException("Backend Provider [" + backend + "] failed to create a new configuration!");
		}
		this.applyExtraTestMethodProperties(configuration);
		return configuration;
	}

	// =====================================================================================================================
	// JUNIT CONTROL
	// =====================================================================================================================

	@Before
	public void onBeforeTest() {
		this.backendTestSuitePlugin = null;
		checkOptOutFromCurrentTest();
		TestSuitePlugin plugin = this.getBackendTestSuitePlugin();
		plugin.onBeforeTest(this.getCurrentTestMethod().getDeclaringClass(), this.getCurrentTestMethod(), this.getTestDirectory());
	}

	@After
	public void onAfterTest(){
		this.getBackendTestSuitePlugin().onAfterTest(this.getCurrentTestMethod().getDeclaringClass(), this.getCurrentTestMethod(), this.getTestDirectory());
		this.backendTestSuitePlugin = null;
		this.backend = null;
	}

	private void checkOptOutFromCurrentTest() {
		DontRunWithBackend classAnnotation = this.getClass().getAnnotation(DontRunWithBackend.class);
		if (classAnnotation != null) {
			// we skip at least one backend; ensure that we are not running on that particular one
			for (String backend : classAnnotation.value()) {
				// "assume" will cause a test to be skipped when the condition applies
				Assume.assumeFalse(backend.equals(this.backend));
			}
		}
		// check if the method is annotated
		Method testMethod = this.getCurrentTestMethod();
		DontRunWithBackend methodAnnotation = testMethod.getAnnotation(DontRunWithBackend.class);
		if(methodAnnotation!= null){
			for(String backend : methodAnnotation.value()){
				Assume.assumeFalse(backend.equals(this.backend));
			}
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER MTHODS
	// =====================================================================================================================


	public Map<String, String> getExtraTestMethodProperties() {
		Method testMethod = this.getCurrentTestMethod();
		Map<String, String> properties = Maps.newHashMap();
		if (testMethod == null) {
			return properties;
		}
		InstantiateChronosWith[] annotations = testMethod.getAnnotationsByType(InstantiateChronosWith.class);
		for (InstantiateChronosWith annotation : annotations) {
			String property = annotation.property();
			String value = annotation.value();
			properties.put(property, value);
		}
		return properties;
	}

	protected void applyExtraTestMethodProperties(final Configuration configuration) {
		Map<String, String> testMethodProperties = this.getExtraTestMethodProperties();
		for (Entry<String, String> entry : testMethodProperties.entrySet()) {
			String property = entry.getKey();
			String value = entry.getValue();
			configuration.setProperty(property, value);
		}
		// ALWAYS apply the debug property
		configuration.setProperty(ChronoDBConfiguration.DEBUG, "true");
		// ALWAYS disable MBeans for testing
		configuration.setProperty(ChronoDBConfiguration.MBEANS_ENABLED, "false");
	}


	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	@Documented
	@Target({ElementType.TYPE, ElementType.METHOD})
	@Retention(RetentionPolicy.RUNTIME)
	public static @interface DontRunWithBackend {

		public String[] value();

	}
}