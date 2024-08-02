from matplotlib import pyplot as plt

def plot():
    data = [0.100,0.095, 0.143, 0.140, 0.208,0.374,0.463,0.605]
    size = [10,100,250,500,1000,2500,5000,10000]

    # Plotting the array
    plt.plot(size,data, marker='o', linestyle='-')

    # Adding labels and title
    plt.title('Time taken to send a number of packets')
    plt.xlabel('Number of requests sent')
    plt.ylabel('Time taken for requests(seconds)')

    # Display the plot
    plt.show()

if __name__ == "__main__":

    # Plot the results
    plot()