;;;;; -*- mode: lisp; syntax: common-lisp; coding: utf-8; base: 10; -*-
;;;;;
;;;;; 
;;;;; Author:      Dan Lentz, Lentz Intergalactic Softworks, Wed May  4 23:23:52 2011
;;;;; Maintainer:  <danlentz@gmail.com>
;;;;;
;;;;;

(in-package :RED)
(in-readtable :RX)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; REDIS MEDIATOR
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (class es) redis-mediator (wilbur-mediator)
  ((host
     :reader redis-host
     :initarg :host
     :initform *redis-host*)
    (port
      :reader redis-port
      :initarg :port
      :initform *redis-port*)
    (redis-db-number
      :reader redis-db-number
      :initarg :db-number
      :initform 0)
    (logger
      :reader redis-logger
      :initarg :logger
      :initform (log:find-logger 'redis))
    (worker
      :reader redis-worker
      :initarg :worker)
    (persistent
      :initform t
      :allocation :class)      
    (redis-version             :reader redis-version                       :initform 0)
    (vm-enabled                :reader redis-vm-enabled                    :initform 0)
    (process-id                :reader redis-process-id                    :initform 0)
    (uptime-in-seconds         :reader redis-uptime-in-seconds             :initform 0)
    (uptime-in-days            :reader redis-uptime-in-days                :initform 0)
    (role                      :reader redis-role                          :initform nil)
    (bgsave-in-progress        :reader redis-bgsave-in-progress            :initform 0)
    (bgrewriteaof-in-progress  :reader redis-bgrewriteaof-in-progress      :initform 0)    
    (last-save-time            :reader redis-last-save-time                :initform 0)
    (changes-since-last-save   :reader redis-changes-since-last-save       :initform 0)
    (connected-clients         :reader redis-connected-clients             :initform 0)
    (connected-slaves          :reader redis-connected-slaves              :initform 0)
    (blocked-clients           :reader redis-blocked-clients               :initform 0)
    (used-memory               :reader redis-used-memory                   :initform 0)
    (used-memory-human         :reader redis-used-memory-human             :initform 0)
    (arch-bits                 :reader redis-arch-bits                     :initform nil)
    (multiplexing-api          :reader redis-multiplexing-api              :initform nil)
    (total-connections         :reader redis-total-connections             :initform 0)
    (total-commands            :reader redis-total-commands                :initform 0)
    (dbsize                    :reader redis-dbsize                        :initform 0)
    (expired-keys              :reader redis-expired-keysa                 :initform 0)
    (pubsub-channels           :reader redis-pubsub-channels               :initform 0)      
    (pubsub-patterns           :reader redis-pubsub-patterns               :initform 0))
  (:default-initargs
   :db (red:edb)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; PROBE-MEDIATOR
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (special-variable e) *probe-p* t)
(def (special-variable)   *probe-interval* 10)

(def (generic e) probe-mediator (mediator))

(def method probe-mediator ((mediator redis-mediator))
  (redis.debug "start-probe: ~A" mediator)
  (redis:with-connection (:host (redis-host mediator) :port (redis-port mediator))
    (setf (slot-value mediator 'dbsize) (redis:red-dbsize)))
  (dolist (pair  (mapcar #'(lambda (field) (ppcre:split #\: field))
                   (mapcar #'(lambda (field) (substitute #\  #\Return
                                               (substitute #\- #\_ (string-upcase field))))
                     (ppcre:split #\Newline
                       (redis:with-connection
                         (:host (redis-host mediator) :port (redis-port mediator))
                         (redis:red-info))))))
    (let ((k  (car pair))
           (v (cadr pair)))
      (when
        (member k (rxi::class-slot-names (cl:find-class 'redis-mediator)) :test #'cl:string-equal)
        (redis.debug "updating slot-value [ ~A => ~A ]" k v)
        (setf (slot-value mediator (intern (car pair) (find-package :red)))
          (subseq v 0 (- (length v) 1)))))))

(def method probe-mediator :after ((mediator redis-mediator))
  (redis.debug "beginning probe-fixup")
  (dolist (slot (list 'pubsub-patterns 'pubsub-channels 'expired-keys 'arch-bits
                  'used-memory 'blocked-clients 'connected-slaves 'connected-clients
                  'changes-since-last-save 'last-save-time 'bgrewriteaof-in-progress
                  'bgsave-in-progress 'uptime-in-days 'uptime-in-seconds 'process-id
                  'vm-enabled))
    (setf (slot-value mediator slot)
      (parse-integer (slot-value mediator slot))))
  (redis.debug "finished probe"))

(def (function) mediator-worker (mediator)
  (loop
    (unless *probe-p* (return-from mediator-worker (get-universal-time)))
    (probe-mediator mediator)
    (sleep *probe-interval*)))
  
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CONSTRUCTOR
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (constructor) redis-mediator ()
  (setf (slot-value -self- 'rxi::default-context)
    (slot-value (mediator-repository -self-) 'db-context))
  (setf (slot-value -self- 'worker) (sb-thread:make-thread #'(lambda () (mediator-worker -self-))
      :name (format nil "redis-worker: DB~D" (slot-value -self- 'redis-db-number))))
  (mediator-add-context -self- (slot-value -self- 'rxi::default-context)))



(def (special-variable e) *redis-mediator* (and (boundp '*redis-mediator*) *redis-mediator*))


(def (function e) redis-mediator (&rest args)
  (or *redis-mediator*
    (setq *redis-mediator*
      (apply #'cl:make-instance (push 'redis-mediator args)))))


(def (symbol-macro) rr (redis-mediator))
(def (symbol-macro) ru::rr (redis-mediator))
(def (symbol-macro) rdf::rr (redis-mediator))
(def (symbol-macro) rxi::rr (redis-mediator))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; UTILITIES
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (with-macro* e) with-redis-mediator-repository (mediator &optional (pipeline t))
  (let ((host (redis-host mediator))
         (port (redis-port mediator))
         (dbnum (redis-db-number mediator)))
    (redis.debug "connection to redis [host: ~A,  port: ~D,  num: ~D]" host port dbnum)
    (redis:with-connection (:host host :port port)
      (redis:red-select dbnum)
      (if pipeline
        (redis:with-pipelining
          (-body-))
        (-body-)))))


(def (function e) ping (redis-mediator)
  (probe-mediator redis-mediator)
  (with-redis-mediator-repository (redis-mediator)
    (redis:red-ping)))

(def (function e) mediator-keys (mediator)
 (red:with-redis-mediator-repository (mediator nil)
   (redis:red-keys "*")))

(def (function e) mediator-values (mediator)
  (red:with-redis-mediator-repository (mediator nil)
    (mapcar #'redis:red-get
      (redis:red-keys "*"))))

(def (function e) mediator-alist (mediator)
  (loop
    :for k :in (mediator-keys mediator)
    :for v :in (mediator-values mediator)
    :collect (cons k v)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CONTEXT
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (generic e) mediator-find-context (mediator context))

(def (method d)  mediator-find-context ((mediator redis-mediator) context)
  (let ((context-key (print-context-key (context-id context))))
    (with-redis-mediator-repository (mediator nil)
      (when (redis:red-exists context-key)
        (values (car (arnesi:ensure-list (redis:red-get context-key))) context-key)))))

(def (generic e) mediator-add-context (mediator context))

(def (method d)  mediator-add-context ((mediator redis-mediator) (context puri:uri))
  (let ((context-key (print-context-key (context-id context))))
    (with-redis-mediator-repository (mediator)
      (redis:red-setnx context-key (princ-to-string context)))
    (values (princ-to-string context) context-key)))

(def (method d)  mediator-add-context ((mediator redis-mediator) (context uuid:uuid))
  (let ((context-key (print-context-key (context-id context))))
    (with-redis-mediator-repository (mediator)
      (redis:red-setnx context-key (uuid-print-urn context)))
    (values (uuid-print-urn context) context-key)))

(def (method d)  mediator-add-context ((mediator redis-mediator) (context string))
  (let ((context-key (print-context-key (context-id context))))
    (with-redis-mediator-repository (mediator)
      (redis:red-setnx context-key context))
    (values context context-key)))



(def (generic e) mediator-delete-context (mediator context))

(def (method d)  mediator-delete-context ((mediator redis-mediator) (context puri:uri))
  (let ((context-key (print-context-key (context-id context))))
    (when (mediator-find-context mediator context)
      (with-redis-mediator-repository (mediator)
        (redis:red-del context-key))
      (values (princ-to-string context) context-key))))

(def (method d)  mediator-delete-context ((mediator redis-mediator) (context uuid:uuid))
  (let ((context-key (print-context-key (context-id context))))
    (when (mediator-find-context mediator context)
      (with-redis-mediator-repository (mediator)
        (redis:red-del context-key))
      (values (uuid-print-urn context) context-key))))

(def (method d)  mediator-delete-context ((mediator redis-mediator) (context string))
  (let ((context-key (print-context-key (context-id context))))
    (when (mediator-find-context mediator context)
      (with-redis-mediator-repository (mediator)
        (redis:red-del context-key))
      (values context context-key))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; NODE
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (generic e) mediator-add-node (mediator node))

(def (method d)  mediator-add-node ((mediator redis-mediator) (node uuid:uuid))
  (let ((key (node-key node))
         (value (uri-namestring node)))
    (with-redis-mediator-repository (mediator)
      (redis:red-setnx key value))
    (values key value)))
  
(def (method d)  mediator-add-node ((mediator redis-mediator) (node puri:uri))
  (let ((key (node-key node))
         (value (princ-to-string node)))
    (with-redis-mediator-repository (mediator)
      (redis:red-setnx key value))
    (values key value)))

(def (method d)  mediator-add-node ((mediator redis-mediator) (node string))
  (let ((key (node-key node))
         (value node))
    (with-redis-mediator-repository (mediator)
      (redis:red-setnx key value))
    (values key value)))

(def (method d)  mediator-add-node ((mediator redis-mediator) (node symbol))
  (let ((key (node-key node))
         (value (uri-namestring node)))
    (with-redis-mediator-repository (mediator)
      (redis:red-setnx key value))
    (values key value)))

(def (method d)  mediator-add-node ((mediator redis-mediator) (node wilbur::literal))
  (let ((key (node-key node))
         (value (princ-to-string node)))
    (with-redis-mediator-repository (mediator)
      (redis:red-setnx key value))
    (values key value)))



(def (generic e) mediator-find-node (mediator key-or-node))

(def (method d)  mediator-find-node ((mediator redis-mediator) key)
  (with-redis-mediator-repository (mediator nil)
    (let* ((actual-key (if (redis:red-exists (node-key key)) (node-key key) key)))
      (if (redis:red-exists actual-key)
        (let ((node-type (first (ppcre:split ":" actual-key)))
               (node (redis:red-get actual-key)))
          (values (cond
                    ((equalp node-type "literal") (read-from-string node))
                    ((equalp node-type "uuid")    node #+()(uuid-parse-urn node))
                    ((equalp node-type "node")    node)
                    (t                            (error "unknown node-type: ~A" node-type)))
            actual-key))))))



(def (generic e) mediator-delete-node (mediator key-or-node))

(def (method d)  mediator-delete-node ((mediator redis-mediator) key)
  (with-redis-mediator-repository (mediator nil)
    (let* ((actual-key (if (redis:red-exists (node-key key)) (node-key key) key))
            (actual-value (if (redis:red-exists actual-key) (redis:red-get actual-key) nil)))
      (if actual-value (redis:red-del actual-key) (return-from mediator-delete-node nil))
      (values actual-key actual-value))))












  
;; (def (method) add-statement* ((  ))
;;   )


;; (def method w:db-add-triple :around ((db edb)(triple wilbur:triple) &optional source)
;;   (call-next-method))
  
;;   (let* ((quad (RDF:QUAD (puri:uri (w:triple-subject actual-triple))))))))))))))))
                               
                
;;(make-instance 'w::edb)
;;                                              #p"library:de;setf;wilbur;schemata;true-rdf-schema.rdf")) 

;; (def method w:db-add-triple ((db persistent-redis-db-mixin)(triple wilbur:triple) &optional source)
;;   (declare (ignorable source))
;;   (multiple-value-bind (actual-triple addedp new-sources) (call-next-method)
;;     (cond (addedp
;;             (with-redis (logv
;;               (dolist (source (or new-sources (list *context*)))
;;                 (let* ((s (w:triple-subject actual-triple))
;;                         (p (w:triple-predicate actual-triple))
;;                         (o (w:triple-object actual-triple))
;;                         (c source)
;;                         (key (compute-spoc-id s p o c))
;;                         (value (list s p o c)))
;;                   (mapc #'(lambda (node)
;;                             (red-rpush key node)) value)))))))))


;; (def class* premium-redis-db (wilbur-premium-db persistent-redis-db-mixin)
;;   ())

;; (def (special-variable e) *pdb* nil)

;; (def (function e) pdb ()
;;   (or *pdb*
;;     (make-instance 'premium-redis-db)))


;; (def method w:db-del-triple :around (())

;;   )
;;;;;
;; Local Variables:
;; indent-tabs: nil
;; outline-regexp: ";;[;]+"
;; End:
;;;;;
