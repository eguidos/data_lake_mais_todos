
#!/usr/bin/env bash

while getopts ":d:l:" OPT; do
    case $OPT in
        d)
	    sudo -u postgres dropdb $OPTARG
	    sudo -u postgres dropuser $OPTARG
	    echo "Postgres user and database dropped."
	    exit
            ;;
        l)
	    sudo -u postgres psql -t $OPTARG
	    exit
            ;;
    esac
done

if [ -z "$1" ]; then
  echo "no database specified. aborting."
  exit
fi

sudo su postgres <<EOF
psql -c "CREATE USER $1 WITH PASSWORD '$1';"
createdb -O$1 -Eutf8 $1
echo "Postgres user and database '$1' created."
EOF