## Testing
Plain
```bash
docker-compose -f docker-compose.db.yml -f docker-compose.tests.yml up
```

With debugger
```bash
./set-dockerhost.sh docker-compose -f docker-compose.db.yml -f docker-compose.tests.yml up
```
