#include <stdio.h>
#include "wbutils.h"

#define FAIL(...) { printf(__VA_ARGS__); printf(" on line %d\n", __LINE__); return false; }
#define EXPECT_TRUE(x) if (!x) FAIL("Expected true, got false")
#define EXPECT_FALSE(x) if (!!x) FAIL("Expected false, got true")
#define ASSERT_INT_EQUALS(x, y) if (x != y) FAIL("%d != %d", x, y)

bool
test_inet_parsing()
{
	hostmask mask;
	EXPECT_TRUE(parse_hostmask("  127.0.0.1/24 ", &mask));
	ASSERT_INT_EQUALS(mask.addr, 0x0100007F);
	ASSERT_INT_EQUALS(mask.mask, 24);

	EXPECT_TRUE(parse_hostmask("127.0.0.2", &mask));
	ASSERT_INT_EQUALS(mask.addr, 0x0200007F);
	ASSERT_INT_EQUALS(mask.mask, 32);

	EXPECT_FALSE(parse_hostmask("127.0.0.foo", &mask));
	//FIXME: EXPECT_FALSE(parse_inet("256.0.0.1", &mask));
	//FIXME: EXPECT_FALSE(parse_inet("127.0.0.1.4", &mask));

	return true;
}

bool
test_hostmask_match()
{
	hostmask mask1, mask2;
	parse_hostmask("1.2.3.4/32", &mask1);

	parse_hostmask("1.2.3.4", &mask2);
	EXPECT_TRUE(match_hostmask(&mask1, mask2.addr));
	parse_hostmask("1.2.3.5", &mask2);
	EXPECT_FALSE(match_hostmask(&mask1, mask2.addr));

	parse_hostmask("1.2.3.4/31", &mask1);
	parse_hostmask("1.2.3.5", &mask2);
	EXPECT_TRUE(match_hostmask(&mask1, mask2.addr));
	parse_hostmask("1.2.3.6", &mask2);
	EXPECT_FALSE(match_hostmask(&mask1, mask2.addr));

	parse_hostmask("1.2.3.4/23", &mask1);
	parse_hostmask("1.2.2.100", &mask2);
	EXPECT_TRUE(match_hostmask(&mask1, mask2.addr));
	parse_hostmask("1.2.4.100", &mask2);
	EXPECT_FALSE(match_hostmask(&mask1, mask2.addr));
	return true;
}

int
main()
{
	int failures = 0;

	failures += !test_inet_parsing();
	failures += !test_hostmask_match();

	printf("Got %d failures\n", failures);
	return failures > 0 ? 1 : 0;
}
