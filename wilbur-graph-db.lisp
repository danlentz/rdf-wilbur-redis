;;;;; -*- mode: common-lisp;   common-lisp-style: modern;    coding: utf-8; -*-
;;;;;
;;;;;
;;;;; redis db
;;;;;
;;;;;   redis and related persistent storage support extensions to wilbur-db
;;;;; 
;;;;; Author:      Dan Lentz, Lentz Intergalactic Softworks, Wed Jul 27 08:22:16 2011
;;;;; Maintainer:  <danlentz@gmail.com>
;;;;;

(in-package :red)
(in-readtable :rx)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SPECIAL VARIABLES
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (special-variable e) *redis-host* #(127 0 0 1))
(def (special-variable e) *redis-port* 6379)
(def (special-variable e) *red-db* (if (boundp '*red-db*) *red-db* nil))

(def (special-variable e) *persist-p* t)
(def (special-variable e) *probe-p* nil)
(def (special-variable e) *probe-interval* 10)


(def (class* eas) node (w::node)
  ())
  
(def (class* eas) literal (w::interned-literal)
  ())

(def (class* eas) statement (w::triple)
  ())


(def (class* eas) db   (w::indexed-db w::interned-literal-db-mixin w::dictionary
                         redis::redis-connection)
  ()
  (:default-initargs :emptyp t :literal-class 'red:literal :node-class 'red:node))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Wilbur DB
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (class eas) red-db (WILBUR::EDB  WILBUR:DICTIONARY  REDIS::REDIS-CONNECTION
                          WILBUR:INTERNED-LITERAL-DB-MIXIN)
  ((context
     :reader  red-db-context
     :initarg :context
     :initform context:default)
    ;; (nodes
    ;;   :reader red-db-nodes
    ;;   :initform (index:make-table :lessp #'string-lessp))
    ;; (literals
    ;;   :reader red-db-literals
    ;;   :initform (index:make-table :lessp  #'string-lessp))
    (logger
      :reader red-db-logger
      :initarg :logger
      :initform (log:find-logger 'redis))
    (worker
      :initform nil
      :initarg :worker)
    (number
      :reader red-db-number
      :initarg :number
      :initform 0)
    (redis-version             :reader red-db-version                       :initform 0)
    (vm-enabled                :reader red-db-vm-enabled                    :initform 0)
    (process-id                :reader red-db-process-id                    :initform 0)
    (uptime-in-seconds         :reader red-db-uptime-in-seconds             :initform 0)
    (uptime-in-days            :reader red-db-uptime-in-days                :initform 0)
    (role                      :reader red-db-role                          :initform nil)
    (bgsave-in-progress        :reader red-db-bgsave-in-progress            :initform 0)
    (bgrewriteaof-in-progress  :reader red-db-bgrewriteaof-in-progress      :initform 0)    
    (last-save-time            :reader red-db-last-save-time                :initform 0)
    (changes-since-last-save   :reader red-db-changes-since-last-save       :initform 0)
    (connected-clients         :reader red-db-connected-clients             :initform 0)
    (connected-slaves          :reader red-db-connected-slaves              :initform 0)
    (blocked-clients           :reader red-db-blocked-clients               :initform 0)
    (used-memory               :reader red-db-used-memory                   :initform 0)
    (used-memory-human         :reader red-db-used-memory-human             :initform 0)
    (arch-bits                 :reader red-db-arch-bits                     :initform nil)
    (multiplexing-api          :reader red-db-multiplexing-api              :initform nil)
    (total-connections         :reader red-db-total-connections             :initform 0)
    (total-commands            :reader red-db-total-commands                :initform 0)
    (dbsize                    :reader red-db-dbsize                        :initform 0)
    (expired-keys              :reader red-db-expired-keys                  :initform 0)
    (pubsub-channels           :reader red-db-pubsub-channels               :initform 0)      
    (pubsub-patterns           :reader red-db-pubsub-patterns               :initform 0))
  (:default-initargs :emptyp t :node-class 'red:node :literal-class 'red:literal))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CONSTRUCTOR
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
       
(def (function e) red-db (&rest args)
  (unless (boundp '*red-db*) (setf *red-db* nil))
  (or *red-db*
    (setf *red-db* (apply #'make-instance (push 'red-db args)))))

;; (red-db)

(def (constructor) red-db ()
  (prog1 -self-
    (debug:spyx 
      (unless (and (slot-boundp -self- 'context) (slot-value -self- 'context))
        (setf (slot-value -self- 'context) context:default))
      (unless (and (slot-boundp -self- 'redis::host) (slot-value -self- 'redis::host))
        (setf (slot-value -self- 'redis::host) *redis-host*))
      (unless (and (slot-boundp -self- 'redis::port) (slot-value -self- 'redis::port))
        (setf (slot-value -self- 'redis::port) *redis-port*))
      (probe-red-db -self-)
      (when (slot-value -self- 'worker)
        (sb-thread:destroy-thread (slot-value -self- 'worker)))
      (setf (slot-value -self- 'worker)
        (sb-thread:make-thread #'(lambda () (red-db-worker -self-))
          :name (format nil "red-worker: ~A ~A ~D"
                  (slot-value -self- 'redis::host)
                  (slot-value -self- 'redis::port)
                  (slot-value -self- 'number))))
      (red-db-add-context -self- (slot-value -self- 'context))
      (setf w:*db* -self-)
      (setf w:*nodes* -self-)
    -self-)))


(def (symbol-macro e) red::db (red-db))
(export 'db)

(def (with-macro* e) with-red-db (&optional (db (red-db)) pipeline)
  (let* ((db-num (red-db-number db))
          (db-host (slot-value db 'redis::host))
          (db-port (slot-value db 'redis::port)))
    (redis:with-connection (:host db-host :port db-port)
      (redis:red-select db-num)
      (if pipeline
        (redis:with-pipelining (-body-))
        (-body-)))))



(def (function e) ping (red-db)
  (probe-red-db  red-db)
  (with-red-db (red-db)
    (redis:red-ping)))

(def (function e) red-db-keys (red-db)
  (with-red-db (red-db)
    (redis:red-keys "*")))

(def (function e) red-db-values (red-db)
  (with-red-db (red-db)
    (mapcar #'redis:red-get
      (redis:red-keys "*"))))

(def (function e) red-db-to-alist (red-db)
  (loop
    :for k :in (red-db-keys red-db)
    :for v :in (red-db-values red-db)
    :collect (cons k v)))

(def (function e) red-db-clear (db)
  (prog2 (with-red-db (db t)
           (redis:red-flushdb))
    (setf *red-db* (make-instance 'red-db))))

;; (red-db-clear db)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; PROBE-MEDIATOR
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (generic e) probe-red-db (red-db))

(def method probe-red-db ((red-db red-db))
  (redis.debug "start-probe: ~A" red-db)
  (with-red-db (red-db)
    (setf (slot-value red-db 'dbsize) (redis:red-dbsize))
    (dolist (pair (mapcar #'(lambda (field) (ppcre:split #\: field))
                    (mapcar #'(lambda (field) (substitute #\  #\Return
                                                (substitute #\- #\_ (string-upcase field))))
                      (ppcre:split #\Newline (redis:red-info)))))
      (let ((k  (car pair))
             (v (cadr pair)))
        (when
          (member k (rxi::class-slot-names (cl:find-class 'red-db)) :test #'cl:string-equal)
          (redis.debug "updating slot-value [ ~A => ~A ]" k v)
          (setf (slot-value red-db (intern (car pair) (find-package :red)))
            (subseq v 0 (- (length v) 1))))))))

(def method probe-red-db :after ((red-db red-db))
  (redis.debug "beginning probe-fixup")
  (dolist (slot (list 'pubsub-patterns 'pubsub-channels 'expired-keys 'arch-bits
                  'used-memory 'blocked-clients 'connected-slaves 'connected-clients
                  'changes-since-last-save 'last-save-time 'bgrewriteaof-in-progress
                  'bgsave-in-progress 'uptime-in-days 'uptime-in-seconds 'process-id
                  'vm-enabled))
    (setf (slot-value red-db slot)
      (parse-integer (slot-value red-db slot))))
  (redis.debug "finished probe"))

(def (function) red-db-worker (red-db)
  (loop
    (unless *probe-p* (return-from red-db-worker (get-universal-time)))
    (probe-red-db red-db)
    (sleep *probe-interval*)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CONTEXT
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
#+()
(def (function e) intern-context (value key)
  (export (setf (symbol-value  (apply #'intern (reverse (mapcar #'string-upcase (ppcre:split ":" key))))) value) :context))

(def (generic e) red-db-find-context (red-db context))

(def (method d)  red-db-find-context ((red-db red-db) context)
  (let ((context-key (print-context-key (context-id context))))
    (with-red-db (red-db nil)
      (when (redis:red-exists context-key)
        (values (car (arnesi:ensure-list (redis:red-get context-key))) context-key)))))

(def (generic e) red-db-add-context (red-db context))

(def (method d)  red-db-add-context ((red-db red-db) (context puri:uri))
  (let ((context-key (print-context-key (context-id context))))
    (with-red-db (red-db)
      (redis:red-setnx context-key (princ-to-string context)))
    (values (princ-to-string context) context-key)))

(def (method d)  red-db-add-context ((red-db red-db) (context uuid:uuid))
  (let ((context-key (print-context-key (context-id context))))
    (with-red-db (red-db)
      (redis:red-setnx context-key (uuid-print-urn context)))
    (values (uuid-print-urn context) context-key)))

(def (method d)  red-db-add-context ((red-db red-db) (context string))
  (let ((context-key (print-context-key (context-id context))))
    (with-red-db (red-db)
      (redis:red-setnx context-key context))
    (values context context-key)))


#+()
(def method red-db-add-context :around ((red-db red-db) context)
  (let ((values-list (multiple-value-list (call-next-method))))
    (apply #'intern-context values-list)
    (apply #'values values-list)))

(red-db-add-context (red-db) (puri:uri "http://example.com/"))
(red-db-add-context (red-db)  "http://example.com/")


(def (generic e) red-db-delete-context (red-db context))

(def (method d)  red-db-delete-context ((red-db red-db) (context puri:uri))
  (let ((context-key (print-context-key (context-id context))))
    (when (red-db-find-context red-db context)
      (with-red-db (red-db)
        (redis:red-del context-key))
      (values (princ-to-string context) context-key))))

(def (method d)  red-db-delete-context ((red-db red-db) (context uuid:uuid))
  (let ((context-key (print-context-key (context-id context))))
    (when (red-db-find-context red-db context)
      (with-red-db (red-db)
        (redis:red-del context-key))
      (values (uuid-print-urn context) context-key))))

(def (method d)  red-db-delete-context ((red-db red-db) (context string))
  (let ((context-key (print-context-key (context-id context))))
    (when (red-db-find-context red-db context)
      (with-red-db (red-db)
        (redis:red-del context-key))
      (values context context-key))))

(defmethod RDF:repository-value ((db red-db) (literal literal))
  (n3:format literal nil))

(rdf:repository-value db #"3"^^!xsd:int)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; NODE
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (generic e) red-db-add-node (red-db node))

(def (method d)  red-db-add-node ((red-db red-db) (node wilbur:node))
    (let ((key (node-key node)) (value (node-uri node)))
    (with-red-db (red-db t)
      (redis:red-setnx key value))
    (values key value)))

;;  (red-db-add-node red-db (w:node-uri node)))

(def (method d)  red-db-add-node ((red-db red-db) (uuid uuid:uuid))
  (red-db-add-node red-db (node :uri (uri-namestring uuid))))
  
(def (method d)  red-db-add-node ((red-db red-db) (uri puri:uri))
  (red-db-add-node red-db (node :uri (princ-to-string uri))))

(def (method d)  red-db-add-node ((red-db red-db) (node string))
  (red-db-add-node red-db (node :uri node)))
  
  ;; (let ((key (node-key node)) (value node))
  ;;   (with-red-db (red-db t)
  ;;     (redis:red-setnx key value))
  ;;   (values key value)))

(def (method d)  red-db-add-node ((red-db red-db) (symbol symbol))
  (red-db-add-node (node (uri-namestring symbol))))

(def (method d)  red-db-add-node ((red-db red-db) (node wilbur::literal))
  (let ((key (node-key node))  (value (n3:format nil node)))
    (with-red-db (red-db t)
      (redis:red-setnx key value))
    (values key value)))

;; (def (method) red-db-add-node :around ((red-db red-db) thing)
;;   (or (red-db-find-node red-db thing)
;;     (call-next-method)))
  
(def (generic e) red-db-find-node (red-db key-or-node))

;; (def (method d)  red-db-find-node ((red-db red-db) key)

;;     (with-red-db (red-db nil)
;;       (let* ((actual-key (if (redis:red-exists (node-key key)) (node-key key) key)))
;;         (if (redis:red-exists actual-key)
;;           (let ((node-type (first (ppcre:split ":" actual-key)))
;;                  (node (redis:red-get actual-key)))
;;             (values (cond
;;                       ((equalp node-type "literal") (read-from-string node))
;;                       ((equalp node-type "uuid")    (node (uuid-parse-urn node)))
;;                       ((equalp node-type "node")    node);;(node node))
;;                       (t                            (error "unknown node-type: ~A" node-type)))
;;               actual-key))))))


;; (def method red-db-find-node :around ((red-db red-db) key)
;; ;;  (or (debug:spyx
;;   ;  (values (index:table-get   (node-key (node key)) (red-db-nodes red-db)) (node-key key))
;;    ;     (values (index:table-get key (red-db-nodes red-db)) key)
;;   (debug:spy
;;     (call-next-method)))

(def (generic e) red-db-delete-node (red-db key-or-node))

(def (method d)  red-db-delete-node ((red-db red-db) key)
  (with-red-db (red-db nil)
    (let* ((actual-key (if (redis:red-exists (node-key key)) (node-key key) key))
            (actual-value (if (redis:red-exists actual-key) (redis:red-get actual-key) nil)))
      (if actual-value (redis:red-del actual-key) (return-from red-db-delete-node nil))
      (values actual-key actual-value))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Wilbur Node
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (function) node (thing)
  (debug:spyx 
    (etypecase thing
      (red:node thing)
      (w::literal (progn
                    (red-db-add-node (red-db) thing)
                    ;;(w::literal  (red-db)
                      thing))
      (null      (make-instance 'red:node :uri (uri-namestring  (uuid:make-v1-uuid)))) 
      (puri:uri  (make-instance 'red:node :uri  (princ-to-string thing)))
      (uuid:uuid (make-instance 'red:node :uri  (uri-namestring  thing)))
      (symbol    (make-instance  'red:node :uri  (uri-namestring  thing)))
      (w:url     (make-instance 'red:node  :uri  (w:url-string    thing)))
      (string    (make-instance 'red::node :uri  thing))
      (w:node    (make-instance 'red::node :uri (uri-namestring thing))))))


(def (constructor) red:node ()

  (setf (id -self-) (node-key -self-))
  (redis.debug "interning node ~A ~a" -self- (id -self-))
  (red-db-add-node (or (node-db -self-) (red-db)) -self-)
;;  (setf (index:table-get (id -self-) (slot-value (node-db -self-) 'nodes)) -self-)
  (redis.debug "interned node ~A" -self-)
  -self-)

(node +http+)
(red:node (puri:uri (cl:short-site-name)))
(red:node (cl:short-site-name))
(red:node (package-name :red))
(red:node (uri-namestring context:x500))
(red:node 'node)
(red:node (w:literal "3.1415926" :datatype !xsd:float))
(red:node (w:literal "415926.1" :datatype !xsd:float))
(red:node '|rdfs|:|Class|)
(red:node (w:node w:-rdf-uri-))
(red:node (w:node w:-rdfs-literal-uri-))
(red:node (w:node w:-rdf-alt-uri-))
(red:node (puri:uri w:-rdf-seq-uri-))
(red:node (w:literal (w:iso8601-date-string (get-universal-time)) :datatype !xsd:dateTime))

(def (function e) delete-class (class-symbol)
  (when (cl:find-class class-symbol :errorp nil)
    (setf (cl:find-class class-symbol) nil)))

(def method w:db-add-triple ((db red-db) (triple wilbur:triple) &optional source)
  (declare (ignorable source))
  (multiple-value-bind (actual-triple addedp new-sources) (call-next-method)
    (with-red-db (db t)
      (dolist (context (or (pushnew source new-sources) (list (db-context db))))
    

(def method w:db-add-triple ((db edb) (triple wilbur:triple) &optional source)
  (declare (ignorable source))
  (multiple-value-bind (actual-triple addedp new-sources) (call-next-method)
    (redis:with-connection (:host (slot-value db 'redis::host) :port (slot-value db 'redis::port))
      (redis:with-pipelining             
        (dolist (context (or (pushnew source new-sources) (list (db-context db))))
          (let* ((s   (w:node-uri  (w:triple-subject actual-triple)))
                  (p  (w:node-uri  (w:triple-predicate actual-triple)))
                  (o  (w:node-uri  (w:triple-object actual-triple)))
                  (c  (typecase context
                        (uuid:uuid (rxi::uri-namestring context))
                        (t context)))
                  (actual-quad (format nil "\"~A\" \"~A\" \"~A\" \"~A\"" s p o c))
                  (id (uuid:print-bytes nil (uuid:make-v5-uuid uuid:+NAMESPACE-URL+ actual-quad)))
                  (key (format nil "spoc:~A:~A"
                         (typecase source
                           (uuid:uuid (uuid:print-bytes nil context))
                           (t         (uuid:print-bytes
                                        (uuid:make-v5-uuid uuid:+NAMESPACE-URL+ context) nil))) id))
                  (value actual-quad))
            (logv:logv
              (redis:red-setnx key value))))))
    (values actual-triple addedp new-sources)))
  


(def method initialize-instance :after ((self edb) &key)
  (redis:with-connection (:host (slot-value self 'redis::host)
                           :port (slot-value self 'redis::port))
    (setf (slot-value self 'w::index-literals-p) t)
    (let* ((keys (redis:red-keys "spoc:*:*")))
      (mapc #'(lambda (key)
                (let* ((statement-string (redis:red-get key)))
                  (with-input-from-string (spoc-string statement-string)
                    (let* ((s (read spoc-string))
                            (p (read spoc-string))
                            (o (read spoc-string))
                            (c (let ((v (read spoc-string)))
                                 (if (string-equal (subseq v 0 4) "urn:")
                                   (uuid:make-uuid-from-string (subseq v 9))
                                   v)))
                              )
                      (w:db-add-triple self (w:triple (w:node s) (w:node p) (w:node o) c))))))
        keys)))
  self)




;;;;;
;; Local Variables:
;; indent-tabs: nil
;; outline-regexp: ";;[;]+"
;; End:
;;;;;
