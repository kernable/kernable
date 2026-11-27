# The shop's kernel in production: TCP with mutual TLS, a key and a grant per service.
# region root
# The root key lives on a machine that stays offline; here the directory offline/ stands in for it.
mkdir -p offline && kernable keys new offline/root >/dev/null
# endregion root
# region keys
mkdir -p data keys && cp offline/root.pub keys/
kernable keys new keys/instance >/dev/null   # the kernel's own key; its vault key is derived from it
for service in worker read-models confirmations guard api; do kernable keys new keys/$service >/dev/null; done
kernable certs new certs worker read-models confirmations guard api   # CA, kernel, one per service
# endregion keys
# region grants
kernable grant offline/root.key keys/worker.pub 'run=shop.*' 'vault_erase=shop.CustomerErased'
kernable grant offline/root.key keys/read-models.pub 'listen=shop.*' 'answer=shop.*' 'vault_read=shop.*'
kernable grant offline/root.key keys/confirmations.pub 'listen=shop.OrderSubmitted' \
  'query=shop.GetOrder,shop.GetCustomer' 'submit=shop.Confirm'
kernable grant offline/root.key keys/guard.pub \
  'listen=shop.CustomerRegistered,shop.CustomerErased,shop.OrderPlaced' 'check=shop.*'
kernable grant offline/root.key keys/api.pub 'submit=shop.*' 'query=shop.*'
# endregion grants
# region start
kernable start --root keys/root.pub --instance-key keys/instance.key \
  --store sqlite:data/shop.db --snapshots sqlite:data/snapshots.db \
  --vault-store sqlite:data/vault.db \
  --listen 0.0.0.0:7233 \
  --tls-cert certs/kernel.tls.crt --tls-key certs/kernel.tls.key --tls-client-ca certs/ca.tls.crt \
  --metrics-listen 127.0.0.1:9233
# endregion start
