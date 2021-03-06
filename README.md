# tdbtool command line (overview)

TESS data base data import tool.
It is being used as part of the [STARS4ALL Project](http://www.stars4all.eu/).

## Description

`tess` is a Linux command line utility to perform some common operations on the TESS database without having to write SQL statements. As this utility modifies the database, it is necessary to invoke it within using `sudo`. Also, you should ensure that the database is not being written by `tessdb` systemd service to avoid *database is locked* exceptions, either by using it at daytime or by pausing the `tessdb` systemd service with `/usr/local/bin/tessdb_pause` and then resume it with `/usr/local/bin/tessdb_resume`.


# INSTALLATION
    
## Requirements

The following components are needed and should be installed first:

 * python 2.7.x (tested on Ubuntu Python 2.7.6) or python 3.6+

### Installation

Installation is done from GitHub:

    git clone https://github.com/astrorafael/tessdb-cmdline.git
    cd tess-cmdline
    sudo python setup.py install

**Note:** Installation from PyPi is now obsolete. Do not use the package uploaded in PyPi.

* All executables are copied to `/usr/local/bin`
* The database is located at `/var/dbase/tess.db` by default, although a diffferent path may be specified.


# COMMANDS

The `tdbtool` command line tool is self-explanatory and has several subcommands. You can find the all by typing `tdbtool --help`
```

```

Each subcommand has its own help that you may display by issuing `tess <subcommand> --help`

Example:
```

```

# USAGE

Recommended sequence of commands:

```
* tdbtool input slurp --csv-file <file>
* tdbtool input differences
* tdbtool stats daily
* tdbtool stats global
* tdbtool show global
* tdbtool input retained --name <name> --period <T> --tolerance <%> --test
* tdbtool input retained --name <name> --period <T> --tolerance <%>
```
