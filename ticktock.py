import time

now = None

def tick():
	global now
	now = time.clock()


def tock(*s):
	global now
	assert now is not None, "Gotta tick before ya can tock!"
	if s:
		print(s[0], ':', round(time.clock() - now, 2), "seconds")
	tick()