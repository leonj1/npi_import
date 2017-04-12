FROM alpine:3.5

ADD npiimport /

ENTRYPOINT [ "/npiimport" ]

