
# Lab 5: GoDB Recovery

**Assigned:**  Tuesday December 5, 2024

**Due:**  Tuesday November 19, 2024 by 11:59 PM


## 1. Getting started

### 1.1. Getting Lab 4

To start Lab 4, there are two methods:

#### The first method-using the standard solution to the previous labs:

1. **Create a new directory** on your machine for the project.

2. Inside this directory, clone the repository by running the following command:
   ```
   git clone https://github.com/UA-DB-Class/go-db-lab-2024-fall.git
   ```

   The `godb` folder contains all the files you need for lab 5, including both the solution to lab 1, lab 2, lab3 and lab4.

#### The second method-using your own solution to the previous labs:

1. **Create a new directory** on your machine for the project.

2. Inside this directory, clone the repository by running the following command:

   ```
   git clone https://github.com/UA-DB-Class/go-db-lab-2024-fall.git
   ```

   The `godb` folder contains all the files you need for lab 4, including both the solution to lab 1, lab 2, lab3 and lab4.

3. Now, move the following files from your Lab 4 work into the `godb` folder you just cloned, to replace the previous same files under this folder. (These files contain all the code written by yourself; and the buffer_pool.go file for lab 4 is very different from the buffer_pool.go file for lab 5, so please use the version provided by lab 5)

   * `heap_page.go`
   * `heap_file.go`
   * `tuple.go`
   * `agg_op.go `
   * `agg_state.go`
   * `delete_op.go`
   * `filter_op.go`
   * `insert_op.go`
   * `join_op.go`
   * `limit_op.go`
   * `order_by_op.go`
   * `project_op.go`
   * `lock_table.go`
   * `waits_for.go`


### 1.2. All you need to finish

This lab only requires completing all the functions in the `buffer_pool_extra.go` and `heap_page_extra_test.go` files. Please refer to the comments above each function in these two files for guidance and implement all the functions in both files. Once implemented, you need to pass all the test functions in the `heap_page_extra_test.go` and `log_file_test.go` test files.
