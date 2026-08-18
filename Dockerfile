FROM alpine:3.22

RUN apk add --no-cache \
    3proxy \
    ca-certificates

COPY 3proxy.cfg /etc/3proxy/3proxy.cfg

EXPOSE 3128

CMD ["3proxy", "/etc/3proxy/3proxy.cfg"]