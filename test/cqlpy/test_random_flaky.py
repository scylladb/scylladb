import random

def test_random_flaky():
  assert(random.randint(0,100) >= 80)