# python_data_science

## Prerequisites for using this repository

### Anaconda
This is the recommended way to have python and juypter notebook installed for use with the files in this repository. You can install anaconda by going to:

https://www.continuum.io/downloads

We use the python 3.5 version for this repository.

#### psycopg2 for Redshift

Once you have anaconda installed, one important package to have is psycopg2, which is used to connect to our Redshift database. To install this, open up your terminal (shortcut is to hit ```⌘ + space``` and type terminal)
then in terminal type ```conda install psycopg2``` and then hit enter. Say yes to any prompts that show up (they don't ask you to sell your soul or anything).

#### set up your environment variables for Redshift

After psycopg2 is done installing, type ```vim .bash_profile``` in your terminal window.
This will open up the .bash_profile file. Then hit the ```I``` key on your keyboard, and now at the bottom left corner of the screen it should say ```INSERT```, this means you are in insert mode. Hit ```return``` on your keyboard to make a new line at the top of the file and then press ```↑``` to go to the newly created line. Then copy and paste the Redshift environment variables that you should have received from my email. Then press ```esc```, and the ```INSERT``` and the bottom left corner of the screen will disappear. Then press ```:```, and a ```:``` should appear at the bottom left corner of the the screen, this is called the command mode. Type ```wq!``` which means write, quit, force, which will save your changes to the .bash_profile file and then exit the editor and go back to your normal terminal. Type ```source .bash_profile``` and this will load your changes into the current terminal window.

### Running the examples

To try out the examples here, type ```jupyter notebook``` in your terminal and this will open up a new tab in your web browser. You can then navigate to this folder (python_data_science) and click any of the files with ```.ipynb``` ending to open a python notebook. There are also files in the subdirectories. Have fun.

### Updating this folder

If for some reason you don't see all the files on your computer that you can see here, it probably means your copy is out of date. In that case type ```git stash``` and then type ```git pull```, and your copy will be updated. This will also overwrite all the changes you may have made to the files, so if you want to save them, then save those files to another location.
