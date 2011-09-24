;;;;; -*- mode: lisp; syntax: common-lisp; coding: utf-8; base: 10; -*-
;;;;;
;;;;; 
;;;;; Author:      Dan Lentz, Lentz Intergalactic Softworks, Wed May  4 23:11:33 2011
;;;;; Maintainer:  <danlentz@gmail.com>
;;;;;


(in-package :CL-USER)


(defpackage "http://ebu.gs/resource/redis/"
  (:nicknames :RED :GS.EBU.RESOURCE.REDIS)
  (:shadowing-import-from :common-lisp
    :find-class
    :set
    :defclass
    :equal
    :type-of
    :ensure-class
    :delete)
  (:shadowing-import-from :de.setf.resource
    :namestring)
  (:use :common-lisp
    :de.setf.resource 
    :hu.dwim.def
    :hu.dwim.logger
    :hu.dwim.stefil
    :named-readtables
    :redis))




;;;;;
;; Local Variables:
;; indent-tabs: nil
;; outline-regexp: ";;[;]+"
;; End:
;;;;;
