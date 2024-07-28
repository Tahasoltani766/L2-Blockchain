import numpy as np

# Define the data type for the structured array
dtype = [('address', 'U50'), ('balance', 'f4'), ('blocknum', 'i4')]

# Step 1: Create an empty structured array
empty_array = np.empty((0,), dtype=dtype)

print("Initial empty array:")
print(empty_array)

# Step 2: Create arrays to append
array1 = np.array([
    ('0x123abc', 100.50, 123456),
    ('0x456def', 200.75, 123457)
], dtype=dtype)

array2 = np.array([
    ('0x789ghi', 300.00, 123458),
    ('0xabc123', 400.25, 123459)
], dtype=dtype)

# Step 3: Append arrays to the initial empty array
combined_array = np.concatenate((empty_array, array2))

print("Combined array after appending:")
print(combined_array)