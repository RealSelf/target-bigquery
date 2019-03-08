pip: prep upload cleanup

prep:
	if [ -d "dist" ]; then mv dist old_dist; fi

dist:
	python3 setup.py sdist bdist_wheel

upload: dist
	python3 -m twine upload dist/*

cleanup:
	if [ -d "dist" ]; then rm -rf old_dist; fi
