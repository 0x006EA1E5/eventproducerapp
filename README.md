tables:
 - name: data
   type: source
   # update-mode: append
   connector:
     type: test-source
     debug: true
     delay: 1
     jitter: 4
     outOfOrder: 2100