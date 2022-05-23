(module
  (type (;0;) (func (param i64) (result i64)))
  (func $dbl (type 0) (param i64) (result i64)
    (local i32 i32 i32 i64 i64 i64 i32 i64 i64 i64 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i64 i64 i64 i64 i64 i32 i32 i64 i64 i64)
    global.get $__stack_pointer
    local.set 1
    i32.const 48
    local.set 2
    local.get 1
    local.get 2
    i32.sub
    local.set 3
    local.get 3
    local.get 0
    i64.store offset=40
    local.get 3
    i64.load offset=40
    local.set 4
    i64.const 32
    local.set 5
    local.get 4
    local.get 5
    i64.shr_s
    local.set 6
    local.get 6
    i32.wrap_i64
    local.set 7
    local.get 3
    local.get 7
    i32.store offset=36
    local.get 3
    i64.load offset=40
    local.set 8
    i64.const 4294967295
    local.set 9
    local.get 8
    local.get 9
    i64.and
    local.set 10
    local.get 10
    i32.wrap_i64
    local.set 11
    local.get 3
    local.get 11
    i32.store offset=32
    memory.size
    local.set 12
    i32.const 16
    local.set 13
    local.get 12
    local.get 13
    i32.shl
    local.set 14
    local.get 3
    local.get 14
    i32.store offset=28
    local.get 3
    i32.load offset=36
    local.set 15
    i32.const 1
    local.set 16
    local.get 15
    local.get 16
    i32.shl
    local.set 17
    i32.const 1
    local.set 18
    local.get 17
    local.get 18
    i32.sub
    local.set 19
    i32.const 65536
    local.set 20
    local.get 19
    local.get 20
    i32.div_s
    local.set 21
    i32.const 1
    local.set 22
    local.get 21
    local.get 22
    i32.add
    local.set 23
    local.get 23
    memory.grow
    drop
    i32.const 0
    local.set 24
    local.get 3
    local.get 24
    i32.store offset=24
    i32.const 0
    local.set 25
    local.get 3
    local.get 25
    i32.store offset=20
    block  ;; label = @1
      loop  ;; label = @2
        local.get 3
        i32.load offset=20
        local.set 26
        local.get 3
        i32.load offset=36
        local.set 27
        local.get 26
        local.set 28
        local.get 27
        local.set 29
        local.get 28
        local.get 29
        i32.lt_s
        local.set 30
        i32.const 1
        local.set 31
        local.get 30
        local.get 31
        i32.and
        local.set 32
        local.get 32
        i32.eqz
        br_if 1 (;@1;)
        local.get 3
        i32.load offset=24
        local.set 33
        local.get 3
        i32.load offset=32
        local.set 34
        local.get 3
        i32.load offset=20
        local.set 35
        local.get 34
        local.get 35
        i32.add
        local.set 36
        local.get 33
        local.get 36
        i32.add
        local.set 37
        local.get 37
        i32.load8_u
        local.set 38
        local.get 3
        i32.load offset=24
        local.set 39
        local.get 3
        i32.load offset=28
        local.set 40
        local.get 3
        i32.load offset=20
        local.set 41
        local.get 40
        local.get 41
        i32.add
        local.set 42
        local.get 39
        local.get 42
        i32.add
        local.set 43
        local.get 43
        local.get 38
        i32.store8
        local.get 3
        i32.load offset=24
        local.set 44
        local.get 3
        i32.load offset=32
        local.set 45
        local.get 3
        i32.load offset=20
        local.set 46
        local.get 45
        local.get 46
        i32.add
        local.set 47
        local.get 44
        local.get 47
        i32.add
        local.set 48
        local.get 48
        i32.load8_u
        local.set 49
        local.get 3
        i32.load offset=24
        local.set 50
        local.get 3
        i32.load offset=28
        local.set 51
        local.get 3
        i32.load offset=36
        local.set 52
        local.get 51
        local.get 52
        i32.add
        local.set 53
        local.get 3
        i32.load offset=20
        local.set 54
        local.get 53
        local.get 54
        i32.add
        local.set 55
        local.get 50
        local.get 55
        i32.add
        local.set 56
        local.get 56
        local.get 49
        i32.store8
        local.get 3
        i32.load offset=20
        local.set 57
        i32.const 1
        local.set 58
        local.get 57
        local.get 58
        i32.add
        local.set 59
        local.get 3
        local.get 59
        i32.store offset=20
        br 0 (;@2;)
      end
    end
    local.get 3
    i32.load offset=36
    local.set 60
    local.get 60
    local.set 61
    local.get 61
    i64.extend_i32_s
    local.set 62
    i64.const 1
    local.set 63
    local.get 62
    local.get 63
    i64.shl
    local.set 64
    i64.const 32
    local.set 65
    local.get 64
    local.get 65
    i64.shl
    local.set 66
    local.get 3
    i32.load offset=28
    local.set 67
    local.get 67
    local.set 68
    local.get 68
    i64.extend_i32_s
    local.set 69
    local.get 66
    local.get 69
    i64.or
    local.set 70
    local.get 3
    local.get 70
    i64.store offset=8
    local.get 3
    i64.load offset=8
    local.set 71
    local.get 71
    return)
  (memory (;0;) 2)
  (global $__stack_pointer (mut i32) (i32.const 66576))
  (global (;1;) i32 (i32.const 1024))
  (export "memory" (memory 0))
  (export "dbl" (func $dbl))
  (export "_scylla_abi" (global 1))
  (data $.rodata (i32.const 1024) "\01"))
