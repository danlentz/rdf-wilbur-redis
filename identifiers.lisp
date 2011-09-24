;;;;; -*- mode: common-lisp;   common-lisp-style: modern;    coding: utf-8; -*-
;;;;;
;;;;;
;;;;; redis auxiliary utilities, integration, and support
;;;;;
;;;;;   facilities related to general redis support, in particular redis key and index api
;;;;; 
;;;;; Author:      Dan Lentz, Lentz Intergalactic Softworks, Wed Jul 27 09:37:56 2011
;;;;; Maintainer:  <danlentz@gmail.com>
;;;;;


(in-package :red)
(in-readtable :rx)




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; UUID Facilities
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (function e) uuid-print-urn (uuid &optional (stream nil))
  (format stream "urn:uuid:~A" uuid))


(def (function e) urn-string-p (string)
  (and (stringp string)
    (eql 45 (length string))
    (string-equal (subseq string 0 9) "urn:uuid:")))


(def (function e) uuid-parse-urn (string)
  (if (urn-string-p string)
    (uuid:make-uuid-from-string (subseq string 9))
    (error " ~A does not conform to format:~% urn:uuid:XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
      string)))


(def (constant e) /uri-pattern-string/ "^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?"
  "The regular-expression from rfc3986. NB. the escaped '?'.")

(def (special-variable e)  *uri-scanner* (ppcre:create-scanner /uri-pattern-string/))



(def (definer) default (symbol value)
  `(unless (and (boundp (quote ,symbol)) ,symbol)
     (progn (defvar ,symbol ,value))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CONTEXT:ID  constituent data
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (generic e)  context-id (context))

(def (method d)   context-id ((uri puri:uri))
  (uuid:print-bytes nil (uuid:make-v5-uuid uuid:+namespace-url+ (princ-to-string uri))))

(def (method d)   context-id ((uuid uuid:uuid))
  (uuid:print-bytes nil uuid))

(def (method d)   context-id ((node w:node))
  (context-id (w::node-uri node)))

(def (method d)   context-id ((context-designator string))
  (if (urn-string-p context-designator)
    (uuid:print-bytes nil (uuid-parse-urn context-designator))
    (uuid:print-bytes nil (uuid:make-v5-uuid uuid:+namespace-url+ context-designator))))

(def (function e) print-context-key (id-string &optional (stream nil))
  (format stream "context:~A" id-string))

(def (function e) context-key (context-designator)
  (print-context-key (context-id context-designator)))
    

(defpackage "http://ebu.gs/resource/context/"
  (:nicknames :context :gs.ebu.resource.context)
  (:export :null :url :oid :x500 :dns :literal :site :default))


(def (default) context:null (uuid:make-null-uuid))
(def (default) context:url uuid:+namespace-url+)
(def (default) context:literal (uuid:make-v5-uuid context:url w:-rdfs-literal-uri-))
(def (default) context:oid uuid:+namespace-oid+)
(def (default) context:dns uuid:+namespace-dns+)
(def (default) context:x500 uuid:+namespace-x500+)
(def (default) context:site (uuid:make-v5-uuid uuid:+namespace-url+ (cl:short-site-name)))
(def (default) context:default context:site)


(def (function e) intern-datatype-context (datatype-designator)
  (setf (symbol-value  (rxi::uri-namestring-identifier datatype-designator))
    #'(lambda (literal)
        (uuid:make-v5-uuid
          (uuid:make-v5-uuid uuid:+NAMESPACE-URL+ (w:node-uri (w:node datatype-designator)))
        literal))))

(intern-datatype-context (w:node-uri !xsd:schema))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; NODE:ID, UUID:ID, and LITERAL:ID constituent data
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (function e) node-id (node-designator)
  (etypecase node-designator
    (wilbur:literal (uuid:print-bytes nil (uuid:make-v5-uuid context:literal
                                            (princ-to-string node-designator))))
    (wilbur:node    (node-id (w:node-uri node-designator)))
    (symbol         (node-id (uri-namestring node-designator)))
    (puri:uri       (node-id (princ-to-string node-designator)))
    (uuid:uuid      (node-id (uri-namestring node-designator)))
  
    (string         (if (urn-string-p node-designator)
                      (uuid:print-bytes nil (uuid-parse-urn node-designator))
                      (uuid:print-bytes nil (uuid:make-v5-uuid uuid:+namespace-url+
                                              node-designator))))))

;;(node (w:literal "2" :datatype !xsd:integer))

(def (function e) print-node-key (node-id &optional stream)
  (format stream "node:~A" node-id))

(def (function e) print-literal-key (literal-id &optional stream)
  (format stream "literal:~A" literal-id))

(def (function e) print-uuid-key (uuid-id &optional stream)
  (format stream "uuid:~A" uuid-id))

(def (function e) node-key (node-designator)
  (typecase node-designator
    (wilbur:literal    (print-literal-key (node-id node-designator)))
    (wilbur:node       (node-key (w:node-uri node-designator)))

    (uuid:uuid         (print-uuid-key    (node-id node-designator)))
    (puri:uri          (node-key (princ-to-string node-designator)))
    (symbol            (node-key (uri-namestring node-designator)))      
    (string            (if (urn-string-p node-designator)
                         (print-uuid-key  (node-id node-designator))
                         (print-node-key    (node-id node-designator))))    
    (t                 (print-node-key    (node-id node-designator)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (SPOC:ID:CONTEXT) Statement Index
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def (function e) spoc-id (subject predicate object &optional (context (uuid:make-null-uuid)))
  (rxi::compute-spoc-sha1-hex-id
    (sb-ext:string-to-octets
      (etypecase subject
        (wilbur:literal (princ-to-string subject))
        (symbol   (uri-namestring subject))
        (string   (if (urn-string-p subject)
                    (uuid:print-bytes nil (uuid-parse-urn subject))
                    subject))
        (puri:uri (princ-to-string subject))
        (uuid:uuid (uuid:print-bytes nil subject))))
    (sb-ext:string-to-octets
      (etypecase predicate
        (symbol   (uri-namestring predicate))
        (string   (if (urn-string-p predicate)
                    (uuid:print-bytes nil (uuid-parse-urn predicate))
                    predicate))
        (puri:uri (princ-to-string predicate))))
    (sb-ext:string-to-octets
      (etypecase object
        (wilbur:literal (princ-to-string object))
        (symbol   (uri-namestring object))
        (string   (if (urn-string-p object)
                    (uuid:print-bytes nil (uuid-parse-urn object))
                    object))
        (puri:uri (princ-to-string object))
        (uuid:uuid (uuid:print-bytes nil object))))
    (sb-ext:string-to-octets (context-id context))))


(def (function e) print-spoc-key (id context &optional stream)
  (format stream "spoc:~A:~A" id context))


(def (function e) spoc-key (s p o &optional (c (uuid:make-null-uuid)))
  (let ((spoc-id (spoc-id s p o c))
         (context-id (context-id c)))
    (print-spoc-key spoc-id context-id)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Single Constituent Aggregated Indices
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(def (function e) print-s-key (subject-id context-id &optional stream)
  (format stream "s:~A:~A" subject-id context-id))

(def (function e) s-key (subject &optional (context (uuid:make-null-uuid)))
  (print-s-key (node-id subject) (context-id context)))


(def (function e) print-p-key (predicate-id context-id &optional stream)
  (format stream "p:~A:~A" predicate-id context-id))

(def (function e) p-key (predicate &optional (context (uuid:make-null-uuid)))
  (print-p-key (node-id predicate) (context-id context)))

(def (function e) print-o-key (object-id context-id &optional stream)
  (format stream "o:~A:~A" object-id context-id))

(def (function e) o-key (object &optional (context (uuid:make-null-uuid)))
  (print-o-key (node-id object) (context-id context)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Compound Constituent Aggregated Indices
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(def (function e) print-po-key (predicate-object-id context-id &optional stream)
  (format stream "po:~A:~A" predicate-object-id context-id))

(def (function e) po-key (predicate object &optional (context (uuid:make-null-uuid)))
  (print-po-key (spoc-id "" predicate object "") (context-id context)))


(def (function e) print-so-key (subject-object-id context-id &optional stream)
  (format stream "so:~A:~A" subject-object-id context-id))

(def (function e) so-key (subject object &optional (context (uuid:make-null-uuid)))
  (print-so-key (spoc-id subject "" object "") (context-id context)))


(def (function e) print-sp-key (subject-predicate-id context-id &optional stream)
  (format stream "sp:~A:~A" subject-predicate-id context-id))

(def (function e) sp-key (subject predicate &optional (context (uuid:make-null-uuid)))
  (print-sp-key (spoc-id subject predicate "" "") (context-id context)))






;;;;
#|

(def (definer :available-flags "e") context (&optional identifier (value (uuid:make-v1-uuid)))
  (typecase identifier
    (null (setf identifier (intern (context-id value) (find-package :context))))
    (puri:uri
      (setf value (uuid:make-v5-uuid uuid:+namespace-url+ (princ-to-string identifier)))
      (setf (symbol-value (rxi::uri-namestring-identifier identifier #'string t)) value))
    (uuid:uuid
      (setf value identifier)
      (setf  (intern (princ-to-string value) :context) value))
    (symbol
      (export `(defparameter (rxi::uri-namestring-identifier (uri-namestring  ,identifier)
                               #'string t) ,value))
      (export `(defparameter (intern (context-id ,identifier) :context) ,value)))
    (string (export `(defparameter (intern (context-id identifier) :context)


    `(cl:export (cl:defparameter (cl:intern (red:context-id ,identifier) :context)                    value))
      
                  (rxi::ensure-uri-package identifier))
                ))
|#
;;;;;
;; Local Variables:
;; indent-tabs: nil
;; outline-regexp: ";;[;]+"
;; End:
;;;;;
