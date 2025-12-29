#/bin/bash

# Instead of building everytime, introduce optional switch to build with target and build-arg
# as arguments
docker build --tag dbt-postgres --target dbt-postgres --build-arg commit_ref=main .

# Add shell argument to specify target
docker run dbt-postgres

exit