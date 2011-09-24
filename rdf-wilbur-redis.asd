;;;;; -*- mode: lisp; syntax: common-lisp; coding: utf-8; base: 10; -*-
;;;;;
;;;;; persistent wilbur db and redis-mediator implementation for de.setf.resource
;;;;; supporting both simple rdf graph storage and CLOS RDF object model interface
;;;;;
;;;;; Author:      Dan Lentz, Lentz Intergalactic Softworks, Wed May  4 23:06:18 2011
;;;;; Maintainer:  <danlentz@gmail.com>
;;;;;
;;;;; Yow!  STYROFOAM..
;;;;;

(in-package :cl-user)

(asdf:defsystem :rdf-wilbur-redis
  :serial t
  :depends-on (:de.setf.resource
                :de.setf.wilbur
                :named-readtables
                :cl-redis
                :hu.dwim.def
                :hu.dwim.logger
                :hu.dwim.stefil)
  :components ((:static-file "rdf-wilbur-redis.asd")
                (:file "package")
                (:file "logger")
                (:file "identifiers")
                (:file "wilbur-graph-db")
                (:file "setf-resource-mediator")
                (:file "test")))


;;;;;
;; Local Variables:
;; indent-tabs: nil
;; outline-regexp: ";;[;]+"
;; End:
;;;;;
