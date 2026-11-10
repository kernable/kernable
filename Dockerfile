# The kernel's image: the `kernable` binary of a release (scripts/release.sh, IMAGE=1) on a
# distroless base, as a user without privileges. The store and the keys live in /data. Outside
# the container the kernel listens with mutual TLS only (plain TCP stays on loopback):
#   docker run --rm -v kernable:/data kernable:<version> certs new /data/tls app
#   docker run -v kernable:/data -p 127.0.0.1:7233:7233 kernable:<version> start --dev \
#     --listen 0.0.0.0:7233 --tls-cert /data/tls/kernel.tls.crt --tls-key /data/tls/kernel.tls.key \
#     --tls-client-ca /data/tls/ca.tls.crt
# Clients use /data/tls/ca.tls.crt and app.tls.crt/.key (KERNABLE_TLS_CA, _CERT, _KEY). --dev is
# for trying it out; production names its root key, kernel key and stores.
# Pinned by digest; update it on purpose (docker buildx imagetools inspect <image>).
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY kernable /usr/local/bin/kernable
WORKDIR /data
VOLUME /data
EXPOSE 7233
ENTRYPOINT ["/usr/local/bin/kernable"]
# Without arguments the image prints the start flags and exits 0 (`kernable --help` is no command
# and exits 2); give `start ...` or another command as above.
CMD ["start", "--help"]
