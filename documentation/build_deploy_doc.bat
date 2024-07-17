: IMPORTANT! 
: A. Make sure you have doxygen installed and in your path. https://doxygen.nl/index.html
: B. Edit DoxyFile.NATS.Client, line 41 to have the version you want. 
: C. Use the version for the commit message in step 7.
: D. Start in directory to that is parent directory to nats.net In my case its \nats
cd \nats

: Step 1. From the doc directory, build the docs, then go back to parent
pushd nats.net\documentation
call doxygen DoxyFile.NATS.Client
popd

: Step 2. From the parent directory clone the repo
rd nats.net-gh-pages /S /Q
git clone https://github.com/nats-io/nats.net.v1 nats.net-gh-pages

: Step 3. Go into the the repo directory
cd nats.net-gh-pages

: Step 4. switch the the gh-pages branch
git switch gh-pages

: Step 5. delete everything that is in there
rd search /S /Q`
del *.* /Q

: Step 6. Copy the _config.yml file
copy ..\nats.net\documentation\_config.yml

: Step 7. Move the generated docs by copying then removing the original
xcopy ..\nats.net\documentation\NATS.Client\html /S
rd ..\nats.net\documentation\NATS.Client /S /Q

: Step 8. git add, commit and push
git add -A
git commit -m "Docs for 1.1.7"
git push origin gh-pages
