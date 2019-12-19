package org.apache.flink.table.sources.lookup.cache;

import org.apache.flink.table.sources.lookup.LookupOptions;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * test for AllCache.
 */
public class AllCacheTest {

	private Cache cache;

	@Before
	public void setUp() throws Exception {
		cache = new AllCache().newInstance();
		LookupOptions lookupOptions = LookupOptions.builder()
			.setCacheType(CacheType.MEMORY)
			.setCacheStrategy(CacheStrategy.ALL)
			.build();
		cache.open(lookupOptions, new TestDataFetcher(), null);
	}

	@Test
	public void test() throws Exception {
		for (int i = 0; i < 100; i++) {
			Row key = Row.of(i);
			List<Row> res = cache.getIfPresent(key);
			Assert.assertNotNull(res);
			Assert.assertEquals(key, res.get(0));
		}
	}

	@After
	public void tearDown() throws Exception {
		cache.close();
	}
}
