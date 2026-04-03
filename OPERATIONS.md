# Operations

## Production logs

The production Docker Compose stack sends container logs to `journald` on each host.

Services:

- `recsys-app`

Useful commands on a production server:

```bash
journalctl CONTAINER_TAG=recsys-app -n 200 --no-pager
journalctl CONTAINER_TAG=recsys-app -f
```

You can still inspect Docker state with:

```bash
cd /home/ai-feed-recommendation-system
docker compose ps
docker compose logs --tail=50 app
```

## Journald persistence

To keep logs across host reboots, the server should have persistent journald storage enabled:

```bash
mkdir -p /var/log/journal
systemctl restart systemd-journald
```

You can verify it with:

```bash
journalctl --disk-usage
```

## Rollback

Production rollback is handled by the GitHub Actions workflow:

- `Rollback Production`

It redeploys a specific GHCR image tag to all production servers in rolling order.
