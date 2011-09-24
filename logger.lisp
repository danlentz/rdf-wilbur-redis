;;;;; -*- mode: common-lisp;   common-lisp-style: modern;    coding: utf-8; -*-
;;;;;
;;;;;
;;;;; logger
;;;;;
;;;;;   hu.dwim.logger facility integration for redis-mediator and related resources
;;;;; 
;;;;; Author:      Dan Lentz, Lentz Intergalactic Softworks, Wed Jul 27 08:25:23 2011
;;;;; Maintainer:  <danlentz@gmail.com>
;;;;;

(in-package :red)
(in-readtable :rx)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; LOG FACILITY
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(def (log:logger e) redis (u:echo))
(red:redis.debug "~A" (redis:with-connection () redis:*connection* ))





;;;;;
;; Local Variables:
;; indent-tabs: nil
;; outline-regexp: ";;[;]+"
;; End:
;;;;;
